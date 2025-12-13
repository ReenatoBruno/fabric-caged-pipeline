import logging

from pyspark.sql import DataFrame, SparkSession
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
            'Error while computing latest copied files.'
        )
        raise e 

