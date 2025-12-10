import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

def read_s3_binary_files(spark: SparkSession, 
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
    
    