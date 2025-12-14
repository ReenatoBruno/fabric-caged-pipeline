import logging

from IPython.display import display
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

def _get_latest_copied_files(df: DataFrame) -> DataFrame: 
    """
    Returns the most recent modification timestamp for each source file.
    """
    logging.info(
        'Starting computation of latest copied files.'
    )
    try: 
        df_latest_files = (
            df.groupBy('source_path')
            .agg(f.max('source_modified_at')
                 .alias('latest_copied_time'))
        )
        logging.info(
            'Successfully computed latest copied timestamps'
        )
        return df_latest_files
    except Exception as e:
        logging.error(
            'Error while computing latest copied files.', 
            exc_info=True
        )
    raise

def _get_new_or_updated_files(df_metadata: DataFrame,
                              df_latest_files: DataFrame) -> DataFrame: 
    """  
    Identifies new or updated files by comparing the current 'source_modified_at'
    against the historic 'latest_copied_time'
    """
    logging.info(
        'Identifying new or updated files based on modification timestamps'
    )
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

def _log_files_to_copy(df: DataFrame) -> DataFrame:
    """
    Logs the number of files that need to be copied and optionally displays them.
    """

    file_count = df.count()

    logging.info(
        f'Files to copy now (new or updated): {file_count}'
    )

    if file_count > 0:
        display(df.orderBy("source_path"))
    else:
        logging.info(
            'No new or updated files found'
        )
    return df
