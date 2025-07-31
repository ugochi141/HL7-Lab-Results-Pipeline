# HL7 Lab Results Pipeline - Enhanced Version

## Overview

The HL7 Lab Results Pipeline is a production-ready Python application for processing HL7 v2.x laboratory result messages (ORU^R01). It parses HL7 messages, identifies critical values, and transforms data for integration with Epic Beaker and Cerner EMR systems.

## Features

- **HL7 Message Parsing**: Robust parsing of HL7 v2.x ORU messages with error handling
- **Critical Value Detection**: Automatic identification of critical lab values with configurable thresholds
- **EMR Integration**: Transform results to Epic Beaker or Cerner formats
- **Asynchronous Processing**: High-performance async/await architecture
- **Comprehensive Logging**: File and console logging with detailed error tracking
- **File I/O Support**: Process individual messages or batch files
- **Output Management**: Organized JSON output with critical value alerts

## Installation

1. Clone the repository:
```bash
git clone https://github.com/ugochi141/HL7-Lab-Results-Pipeline.git
cd HL7-Lab-Results-Pipeline/HL7-Lab-Results-Pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the Demo

```bash
python hl7_enhanced_pipeline.py --demo
```

### Processing a Single HL7 File

```bash
# For Epic format (default)
python hl7_enhanced_pipeline.py --file test_hl7_messages/normal_cbc.hl7

# For Cerner format
python hl7_enhanced_pipeline.py --file test_hl7_messages/critical_chemistry.hl7 --destination cerner
```

### Using as a Library

```python
import asyncio
from hl7_enhanced_pipeline import LabResultsPipeline

async def process_lab_results():
    pipeline = LabResultsPipeline()
    
    # Process a single message
    hl7_message = """MSH|^~\&|LAB|HOSPITAL|EPIC|HOSPITAL|20240715120000||ORU^R01|MSG001|P|2.5|||
PID|1||12345678^^^HOSPITAL^MR||DOE^JOHN^A||19800515|M|||123 MAIN ST^^BALTIMORE^MD^21201||
OBR|1|ORD123456|LAB123456|CBC^COMPLETE BLOOD COUNT|||20240715113000|||||||
OBX|1|NM|WBC^WHITE BLOOD COUNT||8.5|10*3/uL|4.5-11.0|N||F|||20240715115500||"""
    
    result = await pipeline.process_message(hl7_message, 'epic')
    print(result)

asyncio.run(process_lab_results())
```

## Critical Values Configuration

The pipeline monitors these critical values by default:

| Test | Low Limit | High Limit | Units |
|------|-----------|------------|-------|
| Glucose (GLU) | 50 | 400 | mg/dL |
| Potassium (K) | 2.5 | 6.5 | mmol/L |
| Sodium (NA) | 120 | 160 | mmol/L |
| Hemoglobin (HGB) | 7 | 20 | g/dL |
| Platelets (PLT) | 50 | 1000 | 10³/µL |
| White Blood Cells (WBC) | 2 | 50 | 10³/µL |
| pH | 7.2 | 7.6 | - |
| pCO2 | 20 | 60 | mmHg |
| pO2 | 60 | 100 | mmHg |

## Output Structure

### Epic Beaker Format
```json
{
  "PatientID": "12345678",
  "PatientName": "John A Doe",
  "MessageID": "MSG001",
  "Orders": [
    {
      "OrderID": "ORD123456",
      "TestName": "COMPLETE BLOOD COUNT",
      "Results": [
        {
          "ComponentID": "WBC",
          "ComponentName": "WHITE BLOOD COUNT",
          "Value": "8.5",
          "Units": "10*3/uL",
          "ReferenceRange": "4.5-11.0",
          "AbnormalFlag": "N",
          "Status": "F",
          "ResultDate": "2024-07-15T11:55:00",
          "IsCritical": false
        }
      ]
    }
  ]
}
```

### Critical Value Alert
```json
{
  "alert_type": "CRITICAL_LAB_VALUE",
  "timestamp": "2024-07-15T15:00:00",
  "patient_id": "987654321",
  "patient_name": "Michael David Jones",
  "critical_values": [
    {
      "test": "GLUCOSE",
      "value": "32",
      "unit": "mg/dL",
      "reference": "70-100"
    },
    {
      "test": "POTASSIUM",
      "value": "7.2",
      "unit": "mmol/L",
      "reference": "3.5-5.0"
    }
  ]
}
```

## Error Handling

The pipeline includes comprehensive error handling:

- Invalid HL7 message format detection
- Missing required segments handling
- Malformed field recovery
- Encoding issue resolution
- Detailed error logging with context

## Logging

Logs are written to:
- Console (stdout) - INFO level and above
- `hl7_pipeline.log` - All log levels

## Testing

Test HL7 messages are provided in the `test_hl7_messages/` directory:

- `normal_cbc.hl7` - Normal CBC results
- `critical_chemistry.hl7` - Critical chemistry values

## Performance

- Asynchronous processing for high throughput
- Efficient message parsing with minimal memory footprint
- Batch processing capabilities
- Concurrent file I/O operations

## Future Enhancements

- Database integration for result storage
- Real-time alerting system for critical values
- HL7 FHIR support
- REST API endpoint for message submission
- Message queue integration (RabbitMQ/Kafka)
- Dashboard for monitoring pipeline statistics

## Contributing

Please submit issues and pull requests on GitHub.

## License

This project is licensed under the MIT License.