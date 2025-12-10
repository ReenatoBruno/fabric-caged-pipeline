import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from utils.constants import BYTES_PER_MB

def _read_s3_binary_files(spark: SparkSession, 
                          source_path: str) -> DataFrame:
    """
    Read binaty files from s3 bucket
    """
    try: 
        logging.info(
            f'Reading files from {source_path}'
        )
        df_binary_files = (
            spark.read.format('binaryFiles')
            .option('recursiveFileLoockup', True)
            .load(source_path)
        )
        logging.info(
            'The files have benn successfully read from s3 bucket'
        )
        return df_binary_files
    
    except Exception as e:
        logging.exception(
            f'An error occurred while reading files from s3 bucket'
        )
        raise e
    
def _show_schema(df: DataFrame, 
                 show_schema: bool): 
    """
    Show the DataFrame schema if the condition is true
    """
    
    if show_schema:
        df.printSchema()

def _get_metadata(df: DataFrame, 
                  conversion_factor: int) -> DataFrame:
    """
    Select and transform columns for metadata
    """
    logging.info(
        'Retrieving metadata from files...'
    )
    df_metadata = df.select(
        f.col('path').alias('source_path'), 
        f.col('modificationTime').alias('source_modified_at'), 
        f.round(f.col('length') / conversion_factor, 2).alias('source_size_mb')
    )
    return df_metadata