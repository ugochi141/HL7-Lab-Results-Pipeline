import hl7

# Test basic HL7 parsing - messages need \r (carriage return) as segment separator
test_message = "MSH|^~\\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||\rPID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||123 MAIN ST^^BALTIMORE^MD^21201||\rOBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||||||\rOBX|1|NM|WBC^WHITE BLOOD COUNT||15.2|10*3/uL|4.5-11.0|H||F|||20240715115500||\rOBX|2|NM|HGB^HEMOGLOBIN||6.5|g/dL|12.0-16.0|L||F|||20240715115500||\rOBX|3|NM|PLT^PLATELETS||45|10*3/uL|150-400|L||F|||20240715115500||"

# Parse the message
msg = hl7.parse(test_message)

print("Message type:", type(msg))
print("Number of segments:", len(msg))
print("\nSegments found:")
for segment in msg:
    print(f"  {segment[0][0]}")

# Check for PID segment
pid_segments = list(msg.segments('PID'))
print(f"\nNumber of PID segments: {len(pid_segments)}")

if pid_segments:
    pid = pid_segments[0]
    print("PID segment:", pid)
    print("Patient ID field:", pid[3])
else:
    print("No PID segments found!")

# Alternative way to get PID
try:
    pid2 = msg.segment('PID')
    print("\nUsing segment() method:", pid2 is not None)
except:
    print("\nError using segment() method")