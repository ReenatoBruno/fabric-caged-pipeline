import logging
from pyspark.sql import SparkSession

from config.paths import SHORTCUT_PATH
from src.landing.extract_metadata import extract_metadata
from src.landing.build_landing_path import build_landing_path

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
    
    df_landing_path = build_landing_path(df=df_metadata)

    return (df_metadata,
            df_landing_path)