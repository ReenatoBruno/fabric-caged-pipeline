import logging
from IPython.display import display
from pyspark.sql import SparkSession

from config.paths import (SHORTCUT_PATH, 
                          LANDING_ROOT_PATH,
                          LANDING_META_TABLE_NAME)

from src.landing.extract_metadata import extract_metadata
from landing.build_metadata import build_metadata
from landing.setup_metadata_table import setup_meta_table

def main(): 
    
    spark = (
    SparkSession.builder
        .appName('Caged-pipeline')
        .getOrCreate())

    logging.info(
        'Starting CAGED pipeline execution...'
    )

    df_meta_extracted = extract_metadata(spark=spark,
                                         source_path=SHORTCUT_PATH) 
    display(df_meta_extracted.limit(10))
    
    df_metadata = build_metadata(df=df_meta_extracted,
                                 root_path=LANDING_ROOT_PATH)
    display(df_metadata.limit(10))

    df_meta_table = setup_meta_table(spark=spark, 
                                     table_name=LANDING_META_TABLE_NAME)
    (df_meta_table .limit(10))

    return (df_meta_extracted,
            df_metadata,
            df_meta_table)