import logging
from pyspark.sql import SparkSession

from config.paths import (SHORTCUT_PATH, 
                          LANDING_ROOT_PATH,
                          LANDING_META_TABLE_NAME)

from src.landing.extract_metadata import extract_metadata
from src.landing.build_landing_path import build_landing_path
from landing.setup_landing_metadata import setup_landing_metadata

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
    
    df_landing_path = build_landing_path(df=df_metadata,
                                         target_path=LANDING_ROOT_PATH)

    df_meta_table = setup_landing_metadata(spark=spark, 
                                           table_name=LANDING_META_TABLE_NAME)

    return (df_metadata,
            df_landing_path,
            df_meta_table)