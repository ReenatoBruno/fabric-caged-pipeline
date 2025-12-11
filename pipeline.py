import logging
from pyspark.sql import SparkSession

from config.paths import SHORTCUT_PATH
from landing.read_s3_metadata import read_s3_metadata

def main(): 
    
    spark = (
    SparkSession.builder
        .appName('Caged-pipeline')
        .getOrCreate())

    logging.info(
        'Starting CAGED pipeline execution...'
    )

    df_metadata = read_s3_metadata(spark=spark,
                                   source_path=SHORTCUT_PATH) 
    
    return (df_metadata)