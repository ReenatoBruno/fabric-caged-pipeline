import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (StructType, 
                               StructField, 
                               StringType, 
                               TimestampType, 
                               FloatType)

def _define_meta_schema() -> StructType:
    """  
    Define the schama for the landing meta table
    """

    meta_schema = StructType([
        StructField('source_path', StringType(), False), 
        StructField('source_modified_at', TimestampType(), False), 
        StructField('source_size_mb', FloatType(), False), 
        StructField('bucket_path', StringType(), False), 
        StructField('lakehouse_path', StringType(), False)
    ])

    return meta_schema

def _create_meta_table (spark: SparkSession, 
                        schema: StructType, 
                        table_name: str) -> None: 
    """  
    Creates the landing metadata table if it does not already exist.
    """
    logging.info(
        f'Ensuring landing metadata table exists: {table_name}'
    )
    try: 
        (
            spark.createDataFrame([], schema)
            .write
            .format('delta')
            .mode('ignore')
            .saveAsTable(table_name)
        )
        logging.info(
            f'Landing metadata table {table_name} created or already exists'
        )
    except Exception as e: 
        logging.exception(
            f'Failed to create the landing meta table {table_name}', 
            exc_info=True
        )
        raise e 
    
def _get_meta_table(spark: SparkSession, 
                    table_name: str) -> DataFrame:
    """  
    Loads the landing metadata table as a Spark DataFrame
    """
    logging.info(
        f'Loading landing metadata table {table_name}'
        )
    try: 
        df_meta_table = spark.table(table_name)
        logging.info (
            'Successfully loaded landing metadata table {table_name}'
        )
        return df_meta_table
    except Exception as e: 
        logging.error(
            f'Failed to load landing metadata table {table_name}',
            exc_info=True
        )
        raise e