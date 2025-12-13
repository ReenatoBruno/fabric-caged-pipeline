import logging
from IPython.display import display
from pyspark.sql import SparkSession

from config.paths import (SHORTCUT_PATH, 
                          LANDING_ROOT_PATH,
                          LANDING_META_TABLE_NAME)

from src.landing.extract_metadata import extract_metadata
from src.landing.build_landing_path import build_file_meta
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
    display(df_metadata.limit(10))
    
    df_meta_columns = build_file_meta(df=df_metadata,
                                      target_path=LANDING_ROOT_PATH)
    display(df_meta_columns.limit(10))

    df_meta_table = setup_landing_metadata(spark=spark, 
                                           table_name=LANDING_META_TABLE_NAME)
    (df_meta_table .limit(10))

    return (df_metadata,
            df_meta_columns,
            df_meta_table)