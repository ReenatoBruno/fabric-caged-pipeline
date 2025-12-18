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
        df_binary_files = (
            spark.read.format('binaryFile')
            .option('recursiveFileLookup', 'true')
            .load(source_path)
        )
        return df_binary_files
    except Exception as e:
        logging.error( 
            f'FATAL ERROR: An error occurred while reading files from: {source_path}',
            exc_info=True 
        )
        raise 
    
def _show_schema(df: DataFrame, 
                 display_schema: bool) -> None:
    """
    Displays the PySpark DataFrame schema (metadata) if the control flag is set to True.
    """
    
    if display_schema:
        df.printSchema()

def _get_metadata(df: DataFrame, 
                  conversion_factor: int) -> DataFrame:
    """
    Selects, renames, and transforms key file metadata columns.
    """

    df_meta_extracted = df.select(
        f.col('path').alias('source_path'), 
        f.col('modificationTime').alias('source_modified_at'), 
        f.round(f.col('length') / conversion_factor, 2).alias('source_size_mb')
    )
    return df_meta_extracted

def extract_metadata(spark: SparkSession,
                     source_path: str) -> DataFrame:
    """  
    Reading files from Amazon S3 via OneLake Shortcut, and extracts key file metadata.
    """
    
    try:
        logging.info(
            f'Starting metadata extraction process from path: {source_path}'
        )
        logging.info(
            f'Reading binary files from OneLake.'
        )
        df_binary_files = _read_binary_files(spark=spark, 
                                             source_path=source_path)
        
        _show_schema(df=df_binary_files, 
                     display_schema=True)
        
        logging.info(
            'Extracting, renaming, and converting file size metadata.'
        )
        df_meta_extracted = _get_metadata(df=df_binary_files, 
                                          conversion_factor=BYTES_PER_MB)
        
        logging.info(
            'Metadata extraction completed successfully'
        )
        return df_meta_extracted
    except Exception as e:
        logging.error(
            f'FATAL ERROR in {extract_metadata.__name__}: Metadata extraction failed for path {source_path}.',
            exc_info=True
        )
        raise 
