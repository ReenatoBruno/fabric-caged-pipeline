import logging
from pyspark.sql import SparkSession

from config.paths import SHORTCUT_PATH
from src.landing.extract_metadata import extract_metadata

def main(): 
    
    spark = (
    SparkSession.builder
        .appName('Caged-pipeline')
        .getOrCreate())

    logging.info(
        'Starting CAGED pipeline execution...'
    )

    df_metadata = extract_metadata(spark=spark,
                                   source_path=SHORTCUT_PATH) 
    
    return (df_metadata)