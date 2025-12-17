import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, Row
from typing import Iterator, Optional

from utils.helpers import mssparkutils

def _get_schema(spark: SparkSession,
                table_name: str) -> StructType: 
    """
    Fetches the audit table schema for creating the final DataFrame
    """

    schema = spark.table(table_name).schema

    return schema

def _create_output_record(row: Row,
                          status: str = 'SUCCESS',
                          error: Optional[str] = None ) -> dict:
    """ 
    Extracts the fields from the Row and builds the final audit record,
    adding the copied_at timestamp. 
    """

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
    """
    Função isolada, com responsabilidade única:
    processar uma partição inteira.
    """

    results = []
    for row in iterator:
        source_path = row['source_path']
        lakehouse_path = row['lakehouse_path']

        try:
            mssparkutils.fs.cp(source_path, lakehouse_path, True)
            
            results.append(_create_output_record(row))

        except Exception as e:
            logging.exception(
                f'Error while copying {source_path} to {lakehouse_path}'
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
                             table_name: str) -> DataFrame:
    """  
    Orchestrates the distributed file copy process from source paths to the 
    Lakehouse
    """
    
    try: 
        logging.info(
        'Starting file copy orchestration'
        )
        logging.info(
            f'Fetching schema from table {table_name}'
        )
        schema = _get_schema(spark=spark,
                            table_name=table_name)

        logging.info(
            'Starting distributed file copy using rdd.mapPartitions...'
        )       
        rdd = df.rdd

        records_rdd = rdd.mapPartitions(_process_partition)

        count = records_rdd.count()

        if count == 0:
            logging.info(
                'No files were submitted for copy'
            )
            return spark.createDataFrame([], schema)
        logging.info(
            f'Distributed file processing complete. Total audited records: {count}.'
        )
        df_audit = spark.createDataFrame(records_rdd, schema)
        logging.info(
            'Audit DataFrame created. Displaying the first 20 records.'
        )
        _display_results(df=df_audit)

        logging.info(
            'File copy orchestration completed successfully.'
        )
        return df_audit, count
    except Exception as e:

        logging.error(
            f'FATAL ERROR during orchestration: {e}', 
            exc_info=True
        )
        raise



        

