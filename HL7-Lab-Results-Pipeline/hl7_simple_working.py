#!/usr/bin/env python3
"""
Simplified HL7 Lab Results Pipeline - Working Version
Processes HL7 v2.x ORU messages with critical value detection
"""

import hl7
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HL7LabPipeline:
    """Simple HL7 Lab Results Pipeline"""
    
    def __init__(self):
        # Critical value thresholds
        self.critical_values = {
            'GLU': {'low': 50, 'high': 400, 'name': 'Glucose'},
            'K': {'low': 2.5, 'high': 6.5, 'name': 'Potassium'},
            'NA': {'low': 120, 'high': 160, 'name': 'Sodium'},
            'HGB': {'low': 7, 'high': 20, 'name': 'Hemoglobin'},
            'PLT': {'low': 50, 'high': 1000, 'name': 'Platelets'},
            'WBC': {'low': 2, 'high': 50, 'name': 'White Blood Cells'},
        }
        
    def process_message(self, message_text: str) -> Dict:
        """Process an HL7 message and return structured data"""
        try:
            # Parse the HL7 message
            msg = hl7.parse(message_text)
            
            # Extract basic information
            msh = msg.segment('MSH')
            pid = msg.segment('PID')
            
            # Build result structure
            result = {
                'message_id': str(msh[10]) if msh[10] else 'Unknown',
                'patient_id': str(pid[3][0][0]) if pid and pid[3] else 'Unknown',
                'patient_name': self._get_patient_name(pid) if pid else 'Unknown',
                'test_results': [],
                'critical_values': []
            }
            
            # Process OBX segments (lab results)
            for obx in msg.segments('OBX'):
                test_result = self._process_obx(obx)
                result['test_results'].append(test_result)
                
                # Check for critical values
                if self._is_critical(test_result):
                    result['critical_values'].append(test_result)
                    logger.warning(f"CRITICAL: {test_result['test_name']} = {test_result['value']} {test_result['unit']}")
            
            return {
                'status': 'success',
                'data': result
            }
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def _get_patient_name(self, pid_segment) -> str:
        """Extract patient name from PID segment"""
        try:
            last_name = str(pid_segment[5][0][0]) if pid_segment[5][0] else ""
            first_name = str(pid_segment[5][0][1]) if len(pid_segment[5][0]) > 1 else ""
            return f"{first_name} {last_name}".strip()
        except:
            return "Unknown"
    
    def _process_obx(self, obx_segment) -> Dict:
        """Process OBX segment and extract test result"""
        try:
            # Extract test information
            test_code = str(obx_segment[3][0][0]) if obx_segment[3] else ""
            test_name = str(obx_segment[3][0][1]) if obx_segment[3] and len(obx_segment[3][0]) > 1 else test_code
            
            return {
                'test_code': test_code,
                'test_name': test_name,
                'value': str(obx_segment[5][0]) if obx_segment[5] else "",
                'unit': str(obx_segment[6][0]) if obx_segment[6] else "",
                'reference_range': str(obx_segment[7][0]) if obx_segment[7] else "",
                'abnormal_flag': str(obx_segment[8][0]) if obx_segment[8] else "",
                'status': str(obx_segment[11][0]) if obx_segment[11] else "F"
            }
        except Exception as e:
            logger.error(f"Error processing OBX: {e}")
            return {
                'test_code': 'ERROR',
                'test_name': 'Parse Error',
                'value': '',
                'unit': '',
                'reference_range': '',
                'abnormal_flag': '',
                'status': 'ERR'
            }
    
    def _is_critical(self, test_result: Dict) -> bool:
        """Check if test result is a critical value"""
        test_code = test_result['test_code']
        
        if test_code in self.critical_values:
            try:
                value = float(test_result['value'])
                limits = self.critical_values[test_code]
                return value < limits['low'] or value > limits['high']
            except:
                pass
        
        # Check abnormal flags
        return test_result['abnormal_flag'] in ['HH', 'LL', 'H*', 'L*']

def main():
    """Demo the pipeline with sample messages"""
    
    # Create pipeline instance
    pipeline = HL7LabPipeline()
    
    # Sample HL7 messages with proper formatting
    sample_messages = [
        # Normal CBC
        "MSH|^~\\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||\rPID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||123 MAIN ST^^BALTIMORE^MD^21201||\rOBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||||||\rOBX|1|NM|WBC^WHITE BLOOD COUNT||8.5|10*3/uL|4.5-11.0|N||F|||20240715115500||\rOBX|2|NM|HGB^HEMOGLOBIN||14.2|g/dL|12.0-16.0|N||F|||20240715115500||\rOBX|3|NM|PLT^PLATELETS||250|10*3/uL|150-400|N||F|||20240715115500||",
        
        # Critical values
        "MSH|^~\\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715130000||ORU^R01|MSG002|P|2.5|||\rPID|1||98765432^^^HOSPITAL^MR||SMITH^JANE^B||19750320|F|||456 OAK ST^^BALTIMORE^MD^21201||\rOBR|1|ORD789012|LAB789012|CHEM^CHEMISTRY PANEL|||20240715123000|||||||\rOBX|1|NM|GLU^GLUCOSE||35|mg/dL|70-100|LL||F|||20240715125500||\rOBX|2|NM|K^POTASSIUM||6.8|mmol/L|3.5-5.0|HH||F|||20240715125500||\rOBX|3|NM|HGB^HEMOGLOBIN||6.2|g/dL|12.0-16.0|LL||F|||20240715125500||"
    ]
    
    print("HL7 Lab Results Pipeline - Demo")
    print("=" * 60)
    
    for i, message in enumerate(sample_messages, 1):
        print(f"\nProcessing Message {i}:")
        print("-" * 40)
        
        result = pipeline.process_message(message)
        
        if result['status'] == 'success':
            data = result['data']
            print(f"Patient: {data['patient_name']} (ID: {data['patient_id']})")
            print(f"Total tests: {len(data['test_results'])}")
            print(f"Critical values: {len(data['critical_values'])}")
            
            if data['critical_values']:
                print("\nCRITICAL VALUES:")
                for cv in data['critical_values']:
                    print(f"  - {cv['test_name']}: {cv['value']} {cv['unit']} (Ref: {cv['reference_range']})")
            
            # Save to file
            filename = f"result_{data['message_id']}.json"
            with open(filename, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"\nSaved to: {filename}")
        else:
            print(f"ERROR: {result['error']}")
    
    print("\n" + "=" * 60)
    print("Demo completed!")

if __name__ == "__main__":
    main()