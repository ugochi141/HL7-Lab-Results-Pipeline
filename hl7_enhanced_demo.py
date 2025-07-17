import hl7
import json

print("HL7 Lab Results Integration Pipeline")
print("=" * 50)

# Build HL7 message
segments = [
    r"MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||",
    "PID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||",
    "OBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||",
    "OBX|1|NM|WBC^WHITE BLOOD COUNT||15.2|10*3/uL|4.5-11.0|H||F|||",
    "OBX|2|NM|HGB^HEMOGLOBIN||6.5|g/dL|12.0-16.0|L||F|||",
    "OBX|3|NM|PLT^PLATELETS||45|10*3/uL|150-400|L||F|||"
]

hl7_message = '\r'.join(segments)
parsed = hl7.parse(hl7_message)

# Critical value thresholds
CRITICAL_VALUES = {
    'HGB': {'low': 7.0, 'high': 20.0, 'unit': 'g/dL', 'name': 'Hemoglobin'},
    'PLT': {'low': 50, 'high': 1000, 'unit': 'K/uL', 'name': 'Platelets'},
    'WBC': {'low': 2.0, 'high': 50.0, 'unit': 'K/uL', 'name': 'White Blood Count'}
}

# Extract patient info
pid = parsed.segment('PID')
patient_id = str(pid[3])
patient_name = str(pid[5]).replace('^', ' ')

print(f"\nPatient: {patient_name}")
print(f"Patient ID: {patient_id.split('^')[0]}")

# Process results
print("\nLab Results:")
print("-" * 50)

critical_alerts = []
epic_results = []

for obx in parsed.segments('OBX'):
    # Parse test components
    test_code_full = str(obx[3])
    test_code = test_code_full.split('^')[0]
    test_name = test_code_full.split('^')[1] if '^' in test_code_full else test_code
    
    value = str(obx[5])
    unit = str(obx[6])
    reference = str(obx[7]) if obx[7] else ''
    flag = str(obx[8]) if obx[8] else ''
    
    # Display result
    status = ""
    if flag == 'H':
        status = " ⬆️ HIGH"
    elif flag == 'L':
        status = " ⬇️ LOW"
    
    print(f"{test_name}: {value} {unit}{status}")
    if reference:
        print(f"   Reference: {reference}")
    
    # Check for critical values
    if test_code in CRITICAL_VALUES:
        try:
            val_float = float(value)
            critical = CRITICAL_VALUES[test_code]
            
            if val_float < critical['low']:
                critical_alerts.append({
                    'test': test_name,
                    'value': value,
                    'unit': unit,
                    'type': 'CRITICAL LOW',
                    'threshold': f"< {critical['low']} {critical['unit']}"
                })
            elif val_float > critical['high']:
                critical_alerts.append({
                    'test': test_name,
                    'value': value,
                    'unit': unit,
                    'type': 'CRITICAL HIGH',
                    'threshold': f"> {critical['high']} {critical['unit']}"
                })
        except ValueError:
            pass
    
    # Prepare Epic format
    epic_results.append({
        "ComponentID": test_code,
        "ComponentName": test_name,
        "Value": value,
        "Units": unit,
        "ReferenceRange": reference,
        "AbnormalFlag": flag,
        "Status": "Final"
    })

# Display critical value alerts
if critical_alerts:
    print("\n" + "🚨 " * 10)
    print("CRITICAL VALUES DETECTED - IMMEDIATE NOTIFICATION REQUIRED")
    print("🚨 " * 10)
    for alert in critical_alerts:
        print(f"\n⚠️  {alert['test']}: {alert['value']} {alert['unit']}")
        print(f"   Status: {alert['type']}")
        print(f"   Critical Threshold: {alert['threshold']}")
    print("\n📞 Notify ordering physician immediately per protocol!")

# Epic Beaker format
print("\n\nEpic Beaker Format Output:")
print("-" * 50)
epic_message = {
    "MessageType": "LabResult",
    "PatientIdentifier": patient_id.split('^')[0],
    "PatientName": patient_name,
    "OrderID": str(parsed.segment('OBR')[2]),
    "Results": epic_results
}

print(json.dumps(epic_message, indent=2))

# Summary
print("\n✅ Pipeline Processing Complete!")
print(f"\nSummary:")
print(f"- Total tests processed: {len(epic_results)}")
print(f"- Critical values found: {len(critical_alerts)}")
print(f"- Ready for Epic Beaker transmission")

# Create output files
with open('sample_hl7_output.json', 'w') as f:
    json.dump(epic_message, f, indent=2)
print("\n💾 Output saved to: sample_hl7_output.json")
