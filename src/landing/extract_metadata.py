import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from utils.constants import BYTES_PER_MB

def _read_binary_files(spark: SparkSession, 
                       source_path: str) -> DataFrame:
    """
    Reading binary files from OneLake shortcut
    """
    try: 
        logging.info(
            f'Reading files from {source_path}'
        )
        df_binary_files = (
            spark.read.format('binaryFile')
            .option('recursiveFileLookup', 'true')
            .load(source_path)
        )
        logging.info(
            'Files successfully read from OneLake shortcut'
        )
        return df_binary_files
    except Exception as e:
        logging.exception(
            f'An error occurred while reading files from: {source_path}'
        )
        raise e
    
def _show_schema(df: DataFrame, 
                 show_schema: bool): 
    """
    Displays the PySpark DataFrame schema (metadata) if the control flag is set to True.
    """
    
    if show_schema:
        df.printSchema()

def _get_metadata(df: DataFrame, 
                  conversion_factor: int) -> DataFrame:
    """
    Selects, renames, and transforms key file metadata columns.
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

def extract_metadata(spark: SparkSession,
                     source_path: str) -> DataFrame:
    
    logging.info(
        'Starting metadata extraction...'
    )
    
    df_binary_files = _read_binary_files(spark=spark, 
                                         source_path=source_path)
    
    _show_schema(df=df_binary_files, 
                 show_schema=True)

    df_metadata = _get_metadata(df=df_binary_files, 
                                conversion_factor=BYTES_PER_MB)
    
    logging.info(
        'Metadata extraction completed successfully'
    )
    return df_metadata
