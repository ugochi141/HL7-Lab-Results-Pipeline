# HL7 Lab Results Integration Pipeline

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org)
[![HL7](https://img.shields.io/badge/HL7-v2.5-green.svg)](http://www.hl7.org)

## 🩺 Overview

Production-ready pipeline for processing HL7 v2.x ORU (lab result) messages with support for Epic Beaker and Cerner transformations. Includes critical value detection and real-time alerting capabilities.

## 🚀 Live Demo
**Production Pipeline**: [https://hl7-lab-pipeline.railway.app](https://hl7-lab-pipeline.railway.app)  
**Message Processor**: [https://hl7-processor-api.fly.dev](https://hl7-processor-api.fly.dev)

*Live Features:*
- Real-time HL7 message processing
- Epic Beaker and Cerner format transformations
- Critical value detection and alerting
- Message validation and error handling

## Features

- ✅ Parse HL7 v2.x ORU messages
- ✅ Transform to Epic Beaker format
- ✅ Transform to Cerner format  
- ✅ Critical value detection and alerting
- ✅ Comprehensive error handling
- ✅ Configurable routing rules

## Installation

```bash
pip install -r requirements.txt
