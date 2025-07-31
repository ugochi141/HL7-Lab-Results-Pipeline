import hl7
import json

print("HL7 Lab Results Integration Demo")
print("=" * 50)

# Build HL7 message with proper segment separators
segments = [
    "MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||",
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
    
    status = ""
    if flag == 'H':
        status = "(HIGH)"
    elif flag == 'L':
        status = "(LOW)"
    
    print(f"{test_name}: {value} {unit} {status}")
    
    # Check for critical values
    try:
        val_float = float(value)
        if test_name == "HEMOGLOBIN" and val_float < 7.0:
            critical_values.append(f"{test_name}: {value} {unit} (CRITICAL LOW - Normal: 12-16)")
        elif test_name == "PLATELETS" and val_float < 50:
            critical_values.append(f"{test_name}: {value} {unit} (CRITICAL LOW - Normal: 150-400)")
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
    "PatientID": str(pid[3][0]),
    "PatientName": f"{pid[5][1]} {pid[5][0]}",
    "MessageID": str(parsed.segment('MSH')[10]),
    "Results": []
}

for obx in parsed.segments('OBX'):
    epic_format["Results"].append({
        "TestCode": str(obx[3][0]),
        "TestName": str(obx[3][1]),
        "Value": str(obx[5]),
        "Units": str(obx[6]),
        "AbnormalFlag": str(obx[8]) if obx[8] else 'N',
        "Status": str(obx[11])
    })

print(json.dumps(epic_format, indent=2))

# Show Cerner format too
print("\nTransformation to Cerner Format:")
print("-" * 40)
cerner_format = {
    "person_id": str(pid[3][0]),
    "clinical_events": []
}

for obx in parsed.segments('OBX'):
    cerner_format["clinical_events"].append({
        "event_code": str(obx[3][0]),
        "event_title": str(obx[3][1]),
        "result_val": str(obx[5]),
        "result_units": str(obx[6]),
        "abnormal_ind": 1 if obx[8] else 0
    })

print(json.dumps(cerner_format, indent=2))

print("\n✅ HL7 Lab Results Pipeline Demo Complete!")
