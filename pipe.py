import logging

from datetime import datetime
from IPython.display import display

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, Row

from typing import Iterator, Optional

from utils.constants import BYTES_PER_MB
from utils.helpers import mssparkutils


def _read_binary_files(spark: SparkSession, 
                       source_path: str) -> DataFrame:
    try: 
        df_binary = (
            spark.read.format('binaryFile')
            .option('recursiveFilelookup', 'true')
            .load(source_path)
        )
        return df_binary
    except Exception as e:
        raise e  

def _show_schema(df: DataFrame, 
                 display_schema: bool): 
    
    if display_schema:
        df.printSchema()

def _get_metadata(df: DataFrame, 
                  conversion_factor: int) -> DataFrame:
    
    df_meta_extracted = df.select(
        f.col('path').alias('source_path'), 
        f.col('modificationTime').alias('source_modified_at'), 
        f.round(f.col('length') / conversion_factor, 2).alias('source_size_mb')
    )
    return df_meta_extracted

def extract_metadata(spark: SparkSession, 
                     source_path: str) -> DataFrame:
    """  
    source_path, 
    source_modified_at
    source_size_mb
    """
    
    df_binary = _read_binary_files(spark=spark, 
                                   source_path=source_path)
  
    _show_schema(df=df_binary, 
                 display_schema=True)
  
    df_meta_extracted = _get_metadata(df=df_binary, 
                                      conversion_factor=BYTES_PER_MB)
  
    return df_meta_extracted

# ------------------------------------------------------------------

def _build_relative_bucket_path(df: DataFrame) -> DataFrame:
    """   
    Adds a 'bucket_path' column to the DataFrame by extracting the relative 
    file path from the 'source_path' column after '/files/' (case-insensitive).
    
    Returns None if '/files/' is not found in the path.
    """
    
    df_with_bucket_path = df.withColumn(
        'bucket_path',
        f.when(
            f.lower(f.col('source_path')).contains('/files/'),
            f.regexp_extract(
                f.col('source_path'),
                r'(?i)/files/(.+)', 
                1  
            )
        ).otherwise(f.lit(None))
    )
    
    return df_with_bucket_path

def _build_relative_lakehouse_path(df: DataFrame, 
                                   root_path: str) -> DataFrame: 
    
    df_with_lakehouse_path = df.withColumn(
        'lakehouse_path', 
        f.regexp_replace(
            f.concat(
                f.lit(root_path + '/'), 
                f.split(f.col('source_path'), '/caged/').getItem(1)
            ),
            r'\.\w+$',
            ''
        )
    )
    return df_with_lakehouse_path

#-------------------------------------------------------------------
 
def _build_relative_bucket_path(df: DataFrame) -> DataFrame: 
    """   
    Extract the relative backet path within the Onelake
    """
    try: 
        df_with_bucket_path = df.withColumn(
            'bucket_path', 
            f.expr("split(source_path, '/Files/', 2)[1]")
        )
        return df_with_bucket_path
    except Exception as e: 
        raise e 

def _build_relative_lakehouse_path(df: DataFrame, 
                                   root_path: str) -> DataFrame: 
    
    df_with_lakehouse_path = df.withColumn(
        'lakehouse_path', 
        f.regexp_replace(
            f.concat(
                f.lit(root_path + '/'), 
                f.split(f.col('source_path'), '/caged/').getItem(1)
            ),
            r'\.txt$',
            ''
        )
    )
    return df_with_lakehouse_path

def _select_meta_columns(df: DataFrame) -> DataFrame:
    
    df_metadata = df.select('source_path', 
                            'source_modified_at', 
                            'source_size_mb', 
                            'bucket_path', 
                            'lakehouse_path')
    return df_metadata

def build_metadata(df: DataFrame, 
                   root_path: str) -> DataFrame:
    """ 
    Orchestrates the full landing-path build process and returns the final
    metadata DataFrame containing the columns:
    - source_path
    - source_modified_at
    - source_size_mb
    - bucket_path
    - lakehouse_path
    """

    try: 
        df_with_bucket_path = _build_relative_bucket_path(df=df)

        df_with_lakehouse_path = _build_relative_lakehouse_path(df=df_with_bucket_path, 
                                                                root_path=root_path)
    
        df_metadata = _select_meta_columns(df=df_with_lakehouse_path)

        return df_metadata
    except Exception as e: 
        logging.error(
            f'FATAL ERROR: {build_metadata.__name__}', 
            exc_info=True
        )
        raise 

# ------------------------------------------------------------------

#-------------------------------------------------------------------

def _define_meta_schema() -> StructType: 
    
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

def _create_meta_table(spark: SparkSession, 
                       schema: StructType, 
                       table_name: str) -> None: 
    try: 
        (
            spark.createDataFrame([], schema)
            .write
            .format('delta')
            .mode('ignore')
            .saveAsTable(table_name)
        )
    except Exception as e: 
        raise e 

def _load_meta_table(spark: SparkSession, 
                     table_name: str) -> DataFrame:
    try:
        df_meta_table = spark.table(table_name)
        return df_meta_table
    except Exception as e:
        raise e 
  
def setup_meta_table(spark: SparkSession,
                     df: DataFrame,
                     table_name: str) -> DataFrame: 

   meta_schema = _define_meta_schema(df=DataFrame)
   
   _create_meta_table(spark=spark, 
                      schema=meta_schema,
                      table_name=table_name)

   df_meta_table = _load_meta_table(spark=spark, 
                                    table_name=table_name)

   return df_meta_table

# ------------------------------------------------------------------

#-------------------------------------------------------------------

def _get_latest_copied_files(df: DataFrame) -> DataFrame: 
    
    df_latest_files = (
        df.groupBy('source_path')
        .agg(f.max('source_modified_at')
             .alias('latest_copied_time'))
    )
    return df_latest_files

def _get_new_or_updated_files(df_metadata: DataFrame, 
                              df_latest_files: DataFrame) -> DataFrame: 
    
    df_files_to_copy = (
        df_metadata
        .join(df_latest_files, on='source_path', how='left')
        .filter(
            f.col('source_modified_at') >
            f.coalesce(
                f.col('latest_copied_time'),
                f.lit('1970-01-01 00-00-00')
                .cast('timestamp')
            )
        )
        .select('source_path',
                'source_modified_at', 
                'source_size_mb', 
                'bucket_path', 
                'lakehouse_path'
        )   
    )
    return df_files_to_copy

def _log_files_to_copy(df: DataFrame) -> DataFrame: 
    """
    Checks if the DataFrame containing files to be copied is empty
    """
    
    is_empty = df.isEmpty() 
    
    if not is_empty: 
        num_files = df.count() 
        logging.info(
            f'Starting copy for {num_files} new/updated files.'
        )
        display(df.orderBy('source_path'))
    else:
        logging.info(
            'No new files were found for incremental copy.'
        )
        
    return df

def orchestrate_incremental_copy(df_meta_table: DataFrame,
                                 df_metadata: DataFrame) -> DataFrame:
    
    try: 
        logging.info(
            'Starting orchestration of incremental file identification.'
        )
        
        df_latest_files = _get_latest_copied_files(df=df_meta_table)
        
        df_files_to_copy = _get_new_or_updated_files(df=df_metadata, 
                                                     df_latest_files=df_latest_files)
    
        df_ready_for_copy = _log_files_to_copy(df=df_files_to_copy)

        logging.info(
            'Orchestration of incremental identification completed successfully.'
        )
        return df_ready_for_copy
    except Exception as e:
        logging.error(
            f'FATAL ERROR in {orchestrate_incremental_copy.__name__}. The identification pipeline failed.', 
            exc_info=True)
        raise e
# ------------------------------------------------------------------

#-------------------------------------------------------------------

def _get_schema(spark: SparkSession, 
                table_name: str) -> StructType:
    
    schema = spark.table(table_name).schema
    
    return schema

def _create_output_record(row: Row, 
                          status: str = 'SUCCESS', 
                          error: Optional[str] = None) -> dict: 
    
    return {
        'source_path': row['source_path'], 
        'source_modified_at': row['source_modified_at'], 
        'source_size_mb': row['source_size_mb'], 
        'bucket_path': row['bucket_path'], 
        'lakehouse_path': row['lakehouse_path'], 
        'copied_at': datetime.now(),
        'status': status, 
        'error': error 
    }

def _process_partition(iterator) -> Iterator[dict]: 

    results = []
    for row in iterator:
        source_path = row['source_path']
        lakehouse_path = row['lakehouse_path']

        try: 
            mssparkutils.fs.cp(source_path, lakehouse_path, True)

            results.append(_create_output_record(e))
        except Exception as e: 
            logging.exception(
            )
            results.append(
                _create_output_record(
                    row, 
                    status='ERROR', 
                    error=str(e)
                )
            )
    return iter(results)

def _display_results(df: DataFrame) -> None:
    """
    Displays the results
    """

    df.orderBy('source_path').show(truncate=False)

def orchestrate_file_to_copy(spark: SparkSession,
                             df: DataFrame,
                             table_name: str) -> (DataFrame, int):
    """  
    Orchestrates file migration using fastcp for high-performance directory replication.
    """

    try: 
        
        logging.info(
            'Starting copy orchestration on the Driver.'
        )
        schema = _get_schema(spark=spark, table_name=table_name)

        files_to_copy = df.collect()
        count = len(files_to_copy)

        if count == 0:
            logging.info(
                'No files available for copy.'
            )
            return spark.createDataFrame([], schema), 0

        logging.info(
            f'Processing {count} items...'
        )
        
        audit_records = []

        for row in files_to_copy:
            source = row['source_path']
            target = row['lakehouse_path']
            
            try:
                mssparkutils.fs.cp(source, target)
                
                logging.info(
                    f'Successfully copied: {source} to {target}'
                )
                audit_records.append(_create_output_record(row))
                
            except Exception as e:
                logging.error(
                    f'Failed to copy {source}: {str(e)}'
                )
                audit_records.append(
                    _create_output_record(row, status='ERROR', error=str(e))
                )
        df_audit = spark.createDataFrame(audit_records, schema)
        _display_results(df=df_audit)

        return df_audit, count

    except Exception as e:
        logging.error(
            f'FATAL ERROR: {e}', 
            exc_info=True)
        raise
