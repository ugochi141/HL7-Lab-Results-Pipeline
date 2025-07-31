import hl7
import json
from datetime import datetime

print("HL7 Lab Results Integration Demo")
print("=" * 50)

# Sample HL7 message with critical values
hl7_message = """MSH|^~\\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||
PID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||
OBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||
OBX|1|NM|WBC^WHITE BLOOD COUNT||15.2|10*3/uL|4.5-11.0|H||F|||
OBX|2|NM|HGB^HEMOGLOBIN||6.5|g/dL|12.0-16.0|L||F|||
OBX|3|NM|PLT^PLATELETS||45|10*3/uL|150-400|L||F|||"""

# Parse the message
parsed = hl7.parse(hl7_message)

# Extract patient info
pid = parsed.segment('PID')
print(f"\nPatient: {pid[5][1]} {pid[5][0]}")
print(f"Patient ID: {pid[3][0]}")

# Extract and display results
print("\nLab Results:")
print("-" * 40)

critical_values = []

for obx in parsed.segments('OBX'):
    test_name = str(obx[3][1])
    value = str(obx[5])
    unit = str(obx[6])
    flag = str(obx[8]) if obx[8] else ''
    
    print(f"{test_name}: {value} {unit} {flag}")
    
    # Check for critical values
    if test_name == "HEMOGLOBIN" and float(value) < 7.0:
        critical_values.append(f"{test_name}: {value} {unit} (CRITICAL LOW)")
    elif test_name == "PLATELETS" and float(value) < 50:
        critical_values.append(f"{test_name}: {value} {unit} (CRITICAL LOW)")

# Alert for critical values
if critical_values:
    print("\n🚨 CRITICAL VALUES DETECTED! 🚨")
    for cv in critical_values:
        print(f"  - {cv}")
    print("\nImmediate physician notification required!")

# Transform to Epic format
print("\nEpic Beaker Format:")
print("-" * 40)
epic_format = {
    "PatientID": str(pid[3][0]),
    "Results": []
}

for obx in parsed.segments('OBX'):
    epic_format["Results"].append({
        "TestCode": str(obx[3][0]),
        "TestName": str(obx[3][1]),
        "Value": str(obx[5]),
        "Units": str(obx[6])
    })

print(json.dumps(epic_format, indent=2))
