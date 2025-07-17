import hl7
import json

print("HL7 Lab Results Integration Demo")
print("=" * 50)

# Build HL7 message with proper segment separators
segments = [
    r"MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||",
    "PID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||",
    "OBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||",
    "OBX|1|NM|WBC^WHITE BLOOD COUNT||15.2|10*3/uL|4.5-11.0|H||F|||",
    "OBX|2|NM|HGB^HEMOGLOBIN||6.5|g/dL|12.0-16.0|L||F|||",
    "OBX|3|NM|PLT^PLATELETS||45|10*3/uL|150-400|L||F|||"
]

# Join with carriage return (HL7 standard)
hl7_message = '\r'.join(segments)

# Parse the message
parsed = hl7.parse(hl7_message)

# Extract patient info safely
pid = parsed.segment('PID')
patient_id = str(pid[3][0]) if len(pid) > 3 else "Unknown"

# Handle patient name field which might be structured differently
patient_name = "Unknown Patient"
if len(pid) > 5 and pid[5]:
    if isinstance(pid[5], str):
        patient_name = pid[5]
    else:
        # Try to get last name and first name
        try:
            last_name = str(pid[5][0]) if len(pid[5]) > 0 else ""
            first_name = str(pid[5][1]) if len(pid[5]) > 1 else ""
            patient_name = f"{first_name} {last_name}".strip()
        except:
            patient_name = str(pid[5])

print(f"\nPatient: {patient_name}")
print(f"Patient ID: {patient_id}")

# Extract and display results
print("\nLab Results:")
print("-" * 40)

critical_values = []
results_list = []

for obx in parsed.segments('OBX'):
    test_code = str(obx[3][0]) if len(obx) > 3 else ""
    test_name = str(obx[3][1]) if len(obx) > 3 and len(obx[3]) > 1 else test_code
    value = str(obx[5]) if len(obx) > 5 else ""
    unit = str(obx[6]) if len(obx) > 6 else ""
    flag = str(obx[8]) if len(obx) > 8 and obx[8] else ''
    
    status = ""
    if flag == 'H':
        status = "(HIGH)"
    elif flag == 'L':
        status = "(LOW)"
    
    print(f"{test_name}: {value} {unit} {status}")
    
    # Store for later formatting
    results_list.append({
        'code': test_code,
        'name': test_name,
        'value': value,
        'unit': unit,
        'flag': flag
    })
    
    # Check for critical values
    try:
        val_float = float(value)
        if test_name == "HEMOGLOBIN" and val_float < 7.0:
            critical_values.append(f"{test_name}: {value} {unit} (CRITICAL LOW - Normal: 12-16)")
        elif test_name == "PLATELETS" and val_float < 50:
            critical_values.append(f"{test_name}: {value} {unit} (CRITICAL LOW - Normal: 150-400)")
        elif test_name == "WHITE BLOOD COUNT" and val_float > 50:
            critical_values.append(f"{test_name}: {value} {unit} (CRITICAL HIGH - Normal: 4.5-11)")
    except:
        pass

# Alert for critical values
if critical_values:
    print("\n" + "="*50)
    print("🚨 CRITICAL VALUES DETECTED! 🚨")
    print("="*50)
    for cv in critical_values:
        print(f"  - {cv}")
    print("\nImmediate physician notification required per hospital protocol!")

# Transform to Epic Beaker format
print("\nTransformation to Epic Beaker Format:")
print("-" * 40)
epic_format = {
    "PatientID": patient_id,
    "PatientName": patient_name,
    "MessageID": str(parsed.segment('MSH')[10]) if len(parsed.segment('MSH')) > 10 else "Unknown",
    "Results": []
}

for result in results_list:
    epic_format["Results"].append({
        "TestCode": result['code'],
        "TestName": result['name'],
        "Value": result['value'],
        "Units": result['unit'],
        "AbnormalFlag": result['flag'] if result['flag'] else 'N'
    })

print(json.dumps(epic_format, indent=2))

print("\n✅ HL7 Lab Results Pipeline Demo Complete!")
print("\nThis demonstrates:")
print("- HL7 v2.x message parsing")
print("- Critical value detection") 
print("- EMR format transformation (Epic Beaker)")
print("- Error handling for malformed fields")
