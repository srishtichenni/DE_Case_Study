# DE Case Study - Vehicle Crash Analysis

## Overview
This project analyzes vehicle crash data using PySpark to derive insights on various aspects of accidents, including demographics, vehicle types, and contributing factors.

## Dataset
The dataset consists of 6 CSV files containing information about vehicle crashes

## Analytics Done
The application performs 10 specific analyses:
1. Crashes with more than 2 male fatalities
2. Two-wheelers involved in crashes
3. Top 5 vehicle makes in fatal crashes without airbag deployment
4. Vehicles in hit-and-run incidents with valid driver licenses
5. State with highest number of male-only accidents
6. 3rd to 5th vehicle makes contributing to most injuries/deaths
7. Top ethnic group for each vehicle body style in crashes
8. Top 5 zip codes for alcohol-related crashes
9. Crashes with high damage but no property damage reported
10. Top 5 vehicle makes involved in speeding offenses with specific criteria

## Setup and Execution
1. Install dependencies: `pip install -r requirements.txt`
2. Configure input/output paths in `config/config.yaml`
3. Run the application: `spark-submit main.py`
4. View the results stored in `output/analysis_results_(timestamp).json`

## Development 
- Developed with the use of PySpark DataFrame APIs 
- Followed best practices eg. docstrings for all classes and functions
- Implemented modular design 
- Used config-driven approach for data sources and output destinations

## Output
Results for each analysis are stored in the configured output directory.

