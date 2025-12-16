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
    Define the schema for the landing meta table
    """

    meta_schema = StructType([
        StructField('source_path', StringType(), False), 
        StructField('source_modified_at', TimestampType(), False), 
        StructField('source_size_mb', FloatType(), False), 
        StructField('bucket_path', StringType(), False), 
        StructField('lakehouse_path', StringType(), False),
        StructField('copied_at', TimestampType(), False), 
        StructField('status', StringType(), False),
        StructField('error', StringType(), True)
    ])

    return meta_schema

def _create_meta_table (spark: SparkSession, 
                        schema: StructType, 
                        table_name: str) -> None: 
    """  
    Creates the landing metadata table if it does not already exist.
    """

    try: 
        (
            spark.createDataFrame([], schema)
            .write
            .format('delta')
            .mode('ignore')
            .saveAsTable(table_name)
        )
    except Exception as e: 
        logging.exception(
            f'FATAL ERROR: Failed to create the landing meta table {table_name}', 
            exc_info=True
        )
        raise 
    
def _load_meta_table(spark: SparkSession, 
                     table_name: str) -> DataFrame:
    """  
    Loads the landing metadata table as a Spark DataFrame
    """

    try: 
        df_meta_table = spark.table(table_name)

        return df_meta_table
    except Exception as e: 
        logging.error(
            f'FATAL ERROR: Failed to load landing metadata table {table_name}',
            exc_info=True
        )
        raise 
    
def setup_meta_table(spark: SparkSession, 
                     table_name: str) -> DataFrame: 
    """  
    This function first defines the schema, then attempts to create the table
    if it doesn't exist, and finally returns the table as a Spark DataFrame.
    """

    try: 
        # --- STEP 1: Defining schema ---
        logging.info(
            'Defining the required metadata schema.'
        )
        meta_schema = _define_meta_schema()

        # --- STEP 2: Creating table ---
        logging.info(
            f'Attempting to create Delta Lake table {table_name} if it does not exist.'
        )
        _create_meta_table(spark=spark, 
                           schema=meta_schema, 
                           table_name=table_name)
        logging.info(
            f'Delta Lake table {table_name} created or already exists'
        )
        
        # --- STEP 3: Loading table ---
        logging.info(
            f'Loading meta table {table_name} as a DataFrame for incremental processing.'
        )
        df_meta_table = _load_meta_table(spark=spark, 
                                         table_name=table_name)
        logging.info(
            f'Metadata table setup and load complete for {table_name}'
        )
       
        return df_meta_table
    except Exception as e:
        logging.error(
             f'FATAL ERROR in {setup_meta_table.__name__}: Setup failed completely for table {table_name}.',
             exc_info=True
        )
        raise
