import hl7
import json
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional
import asyncio
import aiofiles
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LabResult:
    """Structured lab result data"""
    patient_id: str
    patient_name: str
    order_id: str
    test_code: str
    test_name: str
    result_value: str
    result_unit: str
    reference_range: str
    abnormal_flag: str
    result_status: str
    observation_datetime: datetime
    performing_lab: str
    
class HL7Parser:
    """Parse HL7 v2.x ORU messages"""
    
    def __init__(self):
        self.critical_values = {
            'GLU': {'low': 50, 'high': 400},  # Glucose
            'K': {'low': 2.5, 'high': 6.5},    # Potassium
            'NA': {'low': 120, 'high': 160},   # Sodium
            'HGB': {'low': 7, 'high': 20},     # Hemoglobin
            'PLT': {'low': 50, 'high': 1000},  # Platelets
            'WBC': {'low': 2, 'high': 50},     # White blood cells
        }
    
    def parse_oru_message(self, message_text: str) -> Dict:
        """Parse HL7 ORU message and extract lab results"""
        try:
            msg = hl7.parse(message_text)
            
            # Extract header information
            msh = msg.segment('MSH')
            pid = msg.segment('PID')
            obr = msg.segment('OBR')
            
            results = {
                'message_id': str(msh[10]),
                'sending_facility': str(msh[4]),
                'receiving_facility': str(msh[6]),
                'message_datetime': self._parse_hl7_datetime(msh[7]),
                'patient_id': str(pid[3][0]),
                'patient_name': f"{pid[5][1]} {pid[5][0]}",
                'order_id': str(obr[2]),
                'test_results': []
            }
            
            # Extract all OBX segments (results)
            for obx in msg.segments('OBX'):
                result = self._parse_obx_segment(obx)
                results['test_results'].append(result)
                
                # Check for critical values
                if self._is_critical_value(result):
                    result['is_critical'] = True
                    logger.warning(f"Critical value detected: {result}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error parsing HL7 message: {e}")
            raise
    
    def _parse_obx_segment(self, obx) -> Dict:
        """Parse individual OBX segment"""
        return {
            'test_code': str(obx[3][0]),
            'test_name': str(obx[3][1]),
            'result_value': str(obx[5]),
            'unit': str(obx[6]) if obx[6] else '',
            'reference_range': str(obx[7]) if obx[7] else '',
            'abnormal_flag': str(obx[8]) if obx[8] else '',
            'result_status': str(obx[11]),
            'observation_datetime': self._parse_hl7_datetime(obx[14])
        }
    
    def _parse_hl7_datetime(self, dt_str: str) -> datetime:
        """Convert HL7 datetime to Python datetime"""
        if not dt_str:
            return datetime.now()
        return datetime.strptime(str(dt_str)[:14], '%Y%m%d%H%M%S')
    
    def _is_critical_value(self, result: Dict) -> bool:
        """Check if result is a critical value"""
        test_code = result['test_code']
        if test_code in self.critical_values:
            try:
                value = float(result['result_value'])
                limits = self.critical_values[test_code]
                return value < limits['low'] or value > limits['high']
            except ValueError:
                return False
        return False

class EMRTransformer:
    """Transform lab results for different EMR systems"""
    
    def to_epic_format(self, lab_result: Dict) -> Dict:
        """Transform to Epic Beaker format"""
        return {
            'PatientID': lab_result['patient_id'],
            'OrderID': lab_result['order_id'],
            'Results': [
                {
                    'ComponentID': r['test_code'],
                    'ComponentName': r['test_name'],
                    'Value': r['result_value'],
                    'Units': r['unit'],
                    'ReferenceRange': r['reference_range'],
                    'AbnormalFlag': r['abnormal_flag'],
                    'Status': r['result_status'],
                    'ResultDate': r['observation_datetime'].isoformat()
                }
                for r in lab_result['test_results']
            ]
        }
    
    def to_cerner_format(self, lab_result: Dict) -> Dict:
        """Transform to Cerner format"""
        return {
            'person_id': lab_result['patient_id'],
            'order_id': lab_result['order_id'],
            'clinical_events': [
                {
                    'event_code': r['test_code'],
                    'event_title': r['test_name'],
                    'result_val': r['result_value'],
                    'result_units': r['unit'],
                    'normal_range': r['reference_range'],
                    'abnormal_ind': 1 if r['abnormal_flag'] else 0,
                    'result_status': r['result_status'],
                    'event_end_dt_tm': r['observation_datetime'].isoformat()
                }
                for r in lab_result['test_results']
            ]
        }

class LabResultsPipeline:
    """Main pipeline for processing lab results"""
    
    def __init__(self):
        self.parser = HL7Parser()
        self.transformer = EMRTransformer()
        self.processed_count = 0
        self.error_count = 0
        
    async def process_message(self, message: str, destination: str = 'epic') -> Dict:
        """Process a single HL7 message"""
        try:
            # Parse HL7 message
            parsed_result = self.parser.parse_oru_message(message)
            
            # Transform based on destination
            if destination.lower() == 'epic':
                transformed = self.transformer.to_epic_format(parsed_result)
            elif destination.lower() == 'cerner':
                transformed = self.transformer.to_cerner_format(parsed_result)
            else:
                transformed = parsed_result
            
            # Check for critical values
            critical_results = [r for r in parsed_result['test_results'] 
                              if r.get('is_critical', False)]
            
            if critical_results:
                await self._handle_critical_values(parsed_result, critical_results)
            
            self.processed_count += 1
            logger.info(f"Processed message {parsed_result['message_id']}")
            
            return {
                'status': 'success',
                'message_id': parsed_result['message_id'],
                'data': transformed,
                'has_critical': len(critical_results) > 0
            }
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing message: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _handle_critical_values(self, result: Dict, critical_values: List[Dict]):
        """Handle critical value notifications"""
        # In real implementation, this would send alerts
        logger.critical(f"CRITICAL VALUES for patient {result['patient_id']}:")
        for cv in critical_values:
            logger.critical(f"  {cv['test_name']}: {cv['result_value']} {cv['unit']}")

# Example usage and demo
async def demo_pipeline():
    """Demonstrate the pipeline with sample messages"""
    
    # Sample HL7 ORU message (simplified)
    sample_message = """MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||
PID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||123 MAIN ST^^BALTIMORE^MD^21201||
OBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||||||
OBX|1|NM|WBC^WHITE BLOOD COUNT||15.2|10*3/uL|4.5-11.0|H||F|||20240715115500||
OBX|2|NM|HGB^HEMOGLOBIN||6.5|g/dL|12.0-16.0|L||F|||20240715115500||
OBX|3|NM|PLT^PLATELETS||45|10*3/uL|150-400|L||F|||20240715115500||"""
    
    pipeline = LabResultsPipeline()
    
    # Process for Epic
    print("Processing for Epic Beaker:")
    epic_result = await pipeline.process_message(sample_message, 'epic')
    print(json.dumps(epic_result, indent=2, default=str))
    
    # Process for Cerner
    print("\nProcessing for Cerner:")
    cerner_result = await pipeline.process_message(sample_message, 'cerner')
    print(json.dumps(cerner_result, indent=2, default=str))
    
    print(f"\nPipeline Statistics:")
    print(f"Processed: {pipeline.processed_count}")
    print(f"Errors: {pipeline.error_count}")

if __name__ == "__main__":
    asyncio.run(demo_pipeline())
