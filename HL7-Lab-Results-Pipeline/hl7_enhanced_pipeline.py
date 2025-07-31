import hl7
import json
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional, Tuple
import asyncio
import aiofiles
from dataclasses import dataclass, asdict
import os
import sys
from pathlib import Path

# Configure logging with both file and console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hl7_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
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
    is_critical: bool = False
    
    def to_dict(self):
        """Convert to dictionary with datetime serialization"""
        data = asdict(self)
        data['observation_datetime'] = self.observation_datetime.isoformat()
        return data

class HL7Parser:
    """Enhanced HL7 v2.x ORU message parser with better error handling"""
    
    def __init__(self):
        self.critical_values = {
            'GLU': {'low': 50, 'high': 400, 'name': 'Glucose'},
            'K': {'low': 2.5, 'high': 6.5, 'name': 'Potassium'},
            'NA': {'low': 120, 'high': 160, 'name': 'Sodium'},
            'HGB': {'low': 7, 'high': 20, 'name': 'Hemoglobin'},
            'PLT': {'low': 50, 'high': 1000, 'name': 'Platelets'},
            'WBC': {'low': 2, 'high': 50, 'name': 'White Blood Cells'},
            'PH': {'low': 7.2, 'high': 7.6, 'name': 'pH'},
            'PCO2': {'low': 20, 'high': 60, 'name': 'pCO2'},
            'PO2': {'low': 60, 'high': 100, 'name': 'pO2'},
        }
    
    def parse_oru_message(self, message_text: str) -> Dict:
        """Parse HL7 ORU message with comprehensive error handling"""
        try:
            # Clean the message text
            message_text = message_text.strip()
            if not message_text:
                raise ValueError("Empty message received")
            
            # Parse HL7 message
            msg = hl7.parse(message_text)
            
            # Extract and validate required segments
            msh = self._get_segment(msg, 'MSH', required=True)
            pid = self._get_segment(msg, 'PID', required=True)
            
            # Build result structure
            results = {
                'message_id': self._safe_extract(msh, 10, 'Unknown'),
                'sending_facility': self._safe_extract(msh, 4, 'Unknown'),
                'receiving_facility': self._safe_extract(msh, 6, 'Unknown'),
                'message_datetime': self._parse_hl7_datetime(self._safe_extract(msh, 7)),
                'patient_id': self._extract_patient_id(pid),
                'patient_name': self._extract_patient_name(pid),
                'orders': []
            }
            
            # Process each OBR (order) segment
            for obr in msg.segments('OBR'):
                order = {
                    'order_id': self._safe_extract(obr, 2, 'Unknown'),
                    'test_name': self._safe_extract(obr, 4, [None, 'Unknown Test'])[1],
                    'test_results': []
                }
                
                # Find OBX segments following this OBR
                obx_segments = self._get_obx_for_obr(msg, obr)
                
                for obx in obx_segments:
                    result = self._parse_obx_segment(obx)
                    
                    # Check for critical values
                    if self._is_critical_value(result):
                        result['is_critical'] = True
                        logger.warning(f"Critical value detected: {result['test_name']} = {result['result_value']} {result['unit']}")
                    
                    order['test_results'].append(result)
                
                results['orders'].append(order)
            
            return results
            
        except Exception as e:
            logger.error(f"Error parsing HL7 message: {e}")
            logger.debug(f"Message content: {message_text[:200]}...")
            raise
    
    def _get_segment(self, msg, segment_name: str, required: bool = False):
        """Safely get a segment from the message"""
        try:
            segment = msg.segment(segment_name)
            if segment is None and required:
                raise ValueError(f"Required segment {segment_name} not found")
            return segment
        except:
            if required:
                raise ValueError(f"Required segment {segment_name} not found")
            return None
    
    def _safe_extract(self, segment, field_num: int, default=None):
        """Safely extract field from segment"""
        try:
            if segment is None:
                return default
            
            field = segment[field_num]
            if field is None or str(field).strip() == '':
                return default
                
            return field
        except (IndexError, AttributeError):
            return default
    
    def _extract_patient_id(self, pid) -> str:
        """Extract patient ID with fallback logic"""
        try:
            # Try primary ID field
            patient_id = str(pid[3][0])
            if patient_id and patient_id != '':
                return patient_id
        except:
            pass
        
        # Fallback to alternate IDs
        try:
            return str(pid[2])
        except:
            return "Unknown"
    
    def _extract_patient_name(self, pid) -> str:
        """Extract patient name with proper formatting"""
        try:
            last_name = str(pid[5][0]) if pid[5][0] else ""
            first_name = str(pid[5][1]) if len(pid[5]) > 1 and pid[5][1] else ""
            middle_name = str(pid[5][2]) if len(pid[5]) > 2 and pid[5][2] else ""
            
            name_parts = [n for n in [first_name, middle_name, last_name] if n]
            return " ".join(name_parts) if name_parts else "Unknown Patient"
        except:
            return "Unknown Patient"
    
    def _get_obx_for_obr(self, msg, obr) -> List:
        """Get OBX segments associated with an OBR"""
        obx_segments = []
        found_obr = False
        
        for segment in msg:
            if segment[0] == obr:
                found_obr = True
                continue
                
            if found_obr:
                if str(segment[0][0]) == 'OBX':
                    obx_segments.append(segment)
                elif str(segment[0][0]) in ['OBR', 'PID', 'MSH']:
                    # Stop when we hit another order or patient
                    break
                    
        return obx_segments
    
    def _parse_obx_segment(self, obx) -> Dict:
        """Parse individual OBX segment with error handling"""
        try:
            # Extract test identification
            test_id = obx[3]
            test_code = str(test_id[0]) if test_id else 'Unknown'
            test_name = str(test_id[1]) if len(test_id) > 1 else test_code
            
            # Extract result value
            result_value = str(obx[5]) if obx[5] else ''
            
            # Handle different value types
            value_type = str(obx[2]) if obx[2] else 'ST'
            if value_type == 'NM':  # Numeric
                result_value = self._clean_numeric_value(result_value)
            
            return {
                'test_code': test_code,
                'test_name': test_name,
                'result_value': result_value,
                'unit': str(obx[6]) if obx[6] else '',
                'reference_range': str(obx[7]) if obx[7] else '',
                'abnormal_flag': str(obx[8]) if obx[8] else '',
                'result_status': str(obx[11]) if obx[11] else 'F',
                'observation_datetime': self._parse_hl7_datetime(obx[14] if obx[14] else None),
                'is_critical': False
            }
        except Exception as e:
            logger.error(f"Error parsing OBX segment: {e}")
            return {
                'test_code': 'ERROR',
                'test_name': 'Parsing Error',
                'result_value': str(e),
                'unit': '', 'reference_range': '', 'abnormal_flag': '',
                'result_status': 'ERR',
                'observation_datetime': datetime.now(),
                'is_critical': False
            }
    
    def _clean_numeric_value(self, value: str) -> str:
        """Clean numeric values for processing"""
        # Remove common non-numeric characters
        value = value.replace('<', '').replace('>', '').strip()
        
        # Try to convert to float to validate
        try:
            float(value)
            return value
        except:
            # Return original if not purely numeric
            return value
    
    def _parse_hl7_datetime(self, dt_str) -> datetime:
        """Parse HL7 datetime with multiple format support"""
        if not dt_str:
            return datetime.now()
            
        dt_str = str(dt_str)
        
        # Try different datetime formats
        formats = [
            '%Y%m%d%H%M%S',
            '%Y%m%d%H%M',
            '%Y%m%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(dt_str[:len(fmt.replace('%', ''))], fmt)
            except:
                continue
                
        # If all formats fail, return current datetime
        logger.warning(f"Could not parse datetime: {dt_str}")
        return datetime.now()
    
    def _is_critical_value(self, result: Dict) -> bool:
        """Enhanced critical value checking"""
        test_code = result['test_code'].upper()
        
        # Check if test is in critical values list
        if test_code not in self.critical_values:
            return False
            
        try:
            # Extract numeric value
            value_str = result['result_value']
            value = float(self._clean_numeric_value(value_str))
            
            # Check against limits
            limits = self.critical_values[test_code]
            is_critical = value < limits['low'] or value > limits['high']
            
            if is_critical:
                logger.warning(
                    f"Critical {limits['name']}: {value} {result['unit']} "
                    f"(Normal: {limits['low']}-{limits['high']})"
                )
                
            return is_critical
            
        except ValueError:
            # Non-numeric result, check abnormal flag
            return result.get('abnormal_flag', '').upper() in ['HH', 'LL', 'H*', 'L*']

class EMRTransformer:
    """Transform lab results for different EMR systems"""
    
    def to_epic_format(self, lab_result: Dict) -> Dict:
        """Transform to Epic Beaker format"""
        epic_data = {
            'PatientID': lab_result['patient_id'],
            'PatientName': lab_result['patient_name'],
            'MessageID': lab_result['message_id'],
            'Orders': []
        }
        
        for order in lab_result.get('orders', []):
            epic_order = {
                'OrderID': order['order_id'],
                'TestName': order['test_name'],
                'Results': []
            }
            
            for result in order['test_results']:
                epic_result = {
                    'ComponentID': result['test_code'],
                    'ComponentName': result['test_name'],
                    'Value': result['result_value'],
                    'Units': result['unit'],
                    'ReferenceRange': result['reference_range'],
                    'AbnormalFlag': result['abnormal_flag'],
                    'Status': result['result_status'],
                    'ResultDate': result['observation_datetime'].isoformat(),
                    'IsCritical': result.get('is_critical', False)
                }
                epic_order['Results'].append(epic_result)
            
            epic_data['Orders'].append(epic_order)
        
        return epic_data
    
    def to_cerner_format(self, lab_result: Dict) -> Dict:
        """Transform to Cerner format"""
        cerner_data = {
            'person_id': lab_result['patient_id'],
            'person_name': lab_result['patient_name'],
            'message_id': lab_result['message_id'],
            'clinical_events': []
        }
        
        for order in lab_result.get('orders', []):
            for result in order['test_results']:
                event = {
                    'order_id': order['order_id'],
                    'event_code': result['test_code'],
                    'event_title': result['test_name'],
                    'result_val': result['result_value'],
                    'result_units': result['unit'],
                    'normal_range': result['reference_range'],
                    'abnormal_ind': 1 if result['abnormal_flag'] else 0,
                    'critical_ind': 1 if result.get('is_critical', False) else 0,
                    'result_status': result['result_status'],
                    'event_end_dt_tm': result['observation_datetime'].isoformat()
                }
                cerner_data['clinical_events'].append(event)
        
        return cerner_data

class LabResultsPipeline:
    """Enhanced pipeline with file I/O and batch processing"""
    
    def __init__(self, output_dir: str = "./output"):
        self.parser = HL7Parser()
        self.transformer = EMRTransformer()
        self.processed_count = 0
        self.error_count = 0
        self.critical_count = 0
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
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
            
            # Count critical values
            critical_results = []
            for order in parsed_result.get('orders', []):
                for result in order['test_results']:
                    if result.get('is_critical', False):
                        critical_results.append(result)
                        self.critical_count += 1
            
            if critical_results:
                await self._handle_critical_values(parsed_result, critical_results)
            
            self.processed_count += 1
            logger.info(f"Successfully processed message {parsed_result['message_id']}")
            
            # Save result to file
            await self._save_result(parsed_result['message_id'], transformed, destination)
            
            return {
                'status': 'success',
                'message_id': parsed_result['message_id'],
                'data': transformed,
                'has_critical': len(critical_results) > 0,
                'critical_count': len(critical_results)
            }
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing message: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'message': message[:100] + '...' if len(message) > 100 else message
            }
    
    async def _save_result(self, message_id: str, data: Dict, destination: str):
        """Save processed result to file"""
        filename = self.output_dir / f"{destination}_{message_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        async with aiofiles.open(filename, 'w') as f:
            await f.write(json.dumps(data, indent=2, default=str))
            
        logger.info(f"Saved result to {filename}")
    
    async def _handle_critical_values(self, result: Dict, critical_values: List[Dict]):
        """Handle critical value notifications"""
        logger.critical(f"CRITICAL VALUES ALERT for patient {result['patient_id']} ({result['patient_name']})")
        
        alert_data = {
            'alert_type': 'CRITICAL_LAB_VALUE',
            'timestamp': datetime.now().isoformat(),
            'patient_id': result['patient_id'],
            'patient_name': result['patient_name'],
            'critical_values': []
        }
        
        for cv in critical_values:
            logger.critical(f"  {cv['test_name']}: {cv['result_value']} {cv['unit']} (Ref: {cv['reference_range']})")
            alert_data['critical_values'].append({
                'test': cv['test_name'],
                'value': cv['result_value'],
                'unit': cv['unit'],
                'reference': cv['reference_range']
            })
        
        # Save critical alert
        alert_file = self.output_dir / f"CRITICAL_ALERT_{result['patient_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        async with aiofiles.open(alert_file, 'w') as f:
            await f.write(json.dumps(alert_data, indent=2))
    
    async def process_file(self, file_path: str, destination: str = 'epic'):
        """Process HL7 messages from a file"""
        logger.info(f"Processing file: {file_path}")
        
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                
            # Split messages (assuming one per file or separated by blank lines)
            messages = content.strip().split('\n\n')
            
            results = []
            for message in messages:
                if message.strip():
                    result = await self.process_message(message, destination)
                    results.append(result)
                    
            return results
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return [{'status': 'error', 'file': file_path, 'error': str(e)}]
    
    def get_statistics(self) -> Dict:
        """Get processing statistics"""
        return {
            'processed': self.processed_count,
            'errors': self.error_count,
            'critical_values': self.critical_count,
            'success_rate': f"{(self.processed_count / (self.processed_count + self.error_count) * 100):.1f}%" if (self.processed_count + self.error_count) > 0 else "N/A"
        }

# Example usage and demo
async def demo_pipeline():
    """Enhanced demo with multiple test cases"""
    
    # Test messages
    test_messages = [
        # Normal results
        r"""MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||
PID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||123 MAIN ST^^BALTIMORE^MD^21201||
OBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||||||
OBX|1|NM|WBC^WHITE BLOOD COUNT||8.5|10*3/uL|4.5-11.0|N||F|||20240715115500||
OBX|2|NM|HGB^HEMOGLOBIN||14.2|g/dL|12.0-16.0|N||F|||20240715115500||
OBX|3|NM|PLT^PLATELETS||250|10*3/uL|150-400|N||F|||20240715115500||""",
        
        # Critical values
        r"""MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715130000||ORU^R01|MSG002|P|2.5|||
PID|1||98765432^^^HOSPITAL^MR||SMITH^JANE^B||19750320|F|||456 OAK ST^^BALTIMORE^MD^21201||
OBR|1|ORD789012|LAB789012|CHEM^CHEMISTRY PANEL|||20240715123000|||||||
OBX|1|NM|GLU^GLUCOSE||35|mg/dL|70-100|LL||F|||20240715125500||
OBX|2|NM|K^POTASSIUM||6.8|mmol/L|3.5-5.0|HH||F|||20240715125500||
OBX|3|NM|NA^SODIUM||135|mmol/L|136-145|N||F|||20240715125500||""",
        
        # Multiple orders
        r"""MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715140000||ORU^R01|MSG003|P|2.5|||
PID|1||55555555^^^HOSPITAL^MR||JOHNSON^ROBERT^C||19901210|M|||789 ELM ST^^BALTIMORE^MD^21201||
OBR|1|ORD111111|LAB111111|CBC^COMPLETE BLOOD COUNT|||20240715133000|||||||
OBX|1|NM|WBC^WHITE BLOOD COUNT||15.2|10*3/uL|4.5-11.0|H||F|||20240715135500||
OBX|2|NM|HGB^HEMOGLOBIN||6.5|g/dL|12.0-16.0|LL||F|||20240715135500||
OBR|2|ORD222222|LAB222222|LYTES^ELECTROLYTES|||20240715133000|||||||
OBX|1|NM|NA^SODIUM||118|mmol/L|136-145|LL||F|||20240715135500||
OBX|2|NM|K^POTASSIUM||2.2|mmol/L|3.5-5.0|LL||F|||20240715135500||"""
    ]
    
    pipeline = LabResultsPipeline("./hl7_output")
    
    print("=" * 80)
    print("HL7 Lab Results Pipeline - Enhanced Demo")
    print("=" * 80)
    
    # Process each test message
    for i, message in enumerate(test_messages, 1):
        print(f"\nProcessing Test Message {i}:")
        print("-" * 40)
        
        # Process for Epic
        epic_result = await pipeline.process_message(message, 'epic')
        
        if epic_result['status'] == 'success':
            print(f"✓ Message ID: {epic_result['message_id']}")
            print(f"  Critical Values: {'Yes' if epic_result['has_critical'] else 'No'}")
            if epic_result['has_critical']:
                print(f"  Critical Count: {epic_result['critical_count']}")
        else:
            print(f"✗ Error: {epic_result['error']}")
    
    # Display statistics
    print("\n" + "=" * 80)
    print("Pipeline Statistics:")
    print("-" * 40)
    stats = pipeline.get_statistics()
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    print(f"\nOutput files saved to: {pipeline.output_dir.absolute()}")

# Create a simple CLI interface
async def main():
    """Main entry point with CLI support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='HL7 Lab Results Pipeline')
    parser.add_argument('--file', help='Process HL7 file')
    parser.add_argument('--destination', default='epic', choices=['epic', 'cerner'], 
                       help='Target EMR system format')
    parser.add_argument('--demo', action='store_true', help='Run demo')
    
    args = parser.parse_args()
    
    if args.demo or (not args.file):
        await demo_pipeline()
    else:
        pipeline = LabResultsPipeline()
        results = await pipeline.process_file(args.file, args.destination)
        
        print(f"\nProcessed {len(results)} messages")
        for result in results:
            if result['status'] == 'success':
                print(f"✓ {result['message_id']}")
            else:
                print(f"✗ Error: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    asyncio.run(main())