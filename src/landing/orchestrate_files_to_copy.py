import logging
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, Row, Optional
from typing import List, Optional, Tuple
from utils.helpers import mssparkutils
from utils.constants import MAX_FILES_ALLOWED

def _validate_file_count(count: int) -> None:
    """
    Validates the number of files to be copied to avoid Driver overload.
    """

    if count == 0:
        return

    if count > MAX_FILES_ALLOWED:
        raise ValueError(
            f'File count ({count}) exceeds safe limit '
            f'({MAX_FILES_ALLOWED}) for Driver-based copy.'
        )

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

def _copy_single_file(row: Row) -> dict:
    """
    Copies a single file using mssparkutils.fs.cp and returns
    the corresponding audit record.
    """
    
    source = row['source_path']
    target = row['lakehouse_path']

    try:
        mssparkutils.fs.cp(source, target, True)

        logging.info(
            f'Successfully copied: {source} to {target}'
        )
        return _create_output_record(row)

    except Exception as e:
        logging.error(
            f'Failed to copy {source} to {target}',
            exc_info=True
        )
        return _create_output_record(row,
                                     status='ERROR',
                                     error=str(e)
        )

def _copy_files_and_collect_audit(files_to_copy: List[Row]) -> List[dict]:
    """
    Iterates over the collected Spark rows, copies each file,
    and collects the audit records.
    """
    
    audit_records: List[dict] = []

    for row in files_to_copy:
        audit_record = _copy_single_file(row)
        audit_records.append(audit_record)

    return audit_records

def _display_results(df: DataFrame) -> None:
    """
    Displays the results
    """

    df.orderBy('source_path').show(truncate=False)

def orchestrate_file_to_copy(spark: SparkSession,
                             df: DataFrame,
                             table_name: str) -> Tuple[DataFrame, int]:
    """
    Orchestrates the file copy process using mssparkutils.fs.cp.

    Spark is used only for planning and auditing.
    File copy is executed on the Driver and is designed
    for LOW volume scenarios in Microsoft Fabric.
    """

    try:
        logging.info(
            'Starting copy orchestration on the Driver.'
        )

        count = df.count()
        _validate_file_count(count)

        if count == 0:
            logging.info(
                'No files available for copy.'
            )
            schema = _get_schema(spark=spark,
                                 table_name=table_name)
            
            return spark.createDataFrame([], schema), 0

        logging.info(f'Processing {count} items...')

        files_to_copy = df.collect()

        audit_records = _copy_files_and_collect_audit(files_to_copy)

        schema = _get_schema(spark=spark,
                             table_name=table_name)

        df_audit = spark.createDataFrame(audit_records, schema)

        _display_results(df=df_audit)

        return df_audit, count
    except Exception as e:
        logging.error(
            f'FATAL ERROR in copy orchestration: {e}',
            exc_info=True
        )

        

