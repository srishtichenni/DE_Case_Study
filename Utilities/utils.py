import yaml
import json
import os
import datetime
from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_config():
    with open("./Config/config.yaml", 'r') as file:
        config = yaml.safe_load(file)
    return config

def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=True)

def save_results(results: dict):
    '''
    Save result to analysis output json file 
    Format: analysis_num: {question: question, result:result}
    New file saved for every run, identified by timestamp in filename
    '''
    config = load_config()
    base_path = config['output']['path']
    path = base_path.split(".")[0]
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    json_path = f"{path}_{timestamp}.json"
    
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=4)
    