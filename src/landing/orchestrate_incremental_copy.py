import logging

from IPython.display import display
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

def _get_latest_copied_files(df: DataFrame) -> DataFrame: 
    """
    Returns the most recent modification timestamp for each source file.
    """
    
    df_latest_files = (
        df.groupBy('source_path')
        .agg(f.max('source_modified_at')
             .alias('latest_copied_time'))
    )
    return df_latest_files

def _get_new_or_updated_files(df_metadata: DataFrame,
                              df_latest_files: DataFrame) -> DataFrame: 
    """  
    Identifies new or updated files by comparing the current 'source_modified_at'
    against the historic 'latest_copied_time'
    """
    logging.info(
        'Identifying new or updated files based on modification timestamps'
    )
    try: 
        df_files_to_copy = (
            df_metadata
            .join(df_latest_files, on='source_path', how='left')
            .filter(
                f.col('source_modified_at') >
                f.coalesce(
                    f.col('latest_copied_time'), 
                    f.lit('1970-01-01 00:00:00')
                    .cast('timestamp'))
            )
            .select('source_path', 'lakehouse_path')
        )
        return df_files_to_copy
    except Exception as e: 
        logging.exception(
            'Failed to identify new or updated files'
        )
        raise e

def _log_files_to_copy(df: DataFrame) -> DataFrame:
    """
    Logs whether there are files to copy and optionally displays them
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
    
    df_latest_files = _get_latest_copied_files(df=df_meta_table)
    
    df_files_to_copy = _get_new_or_updated_files(df=df_metadata, 
                                                 df_latest_files=df_latest_files)
    
    df_ready_for_copy = _log_files_to_copy(df=df_files_to_copy)

    return df_ready_for_copy