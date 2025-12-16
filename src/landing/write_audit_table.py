import logging
from pyspark.sql import DataFrame

def write_audit_table(df: DataFrame, 
                      table_name: str,
                      count: int) -> None:
    """
    Loads the audit records from the DataFrame 
    to the target Delta table (history table), using 'append' mode.
    """

    try:
        logging.info(
            f'Writing {count} audit records to {table_name}'
        )
        if count > 0:
            (
                df.write
                .format('delta')
                .mode('append')
                .saveAsTable(table_name)
            )
            logging.info(
                f'Audit records successfully written to {table_name}'
            )
        else:
            logging.info(
                'No audit records to write. Skipping Delta operation.'
            )
    except Exception:
        logging.exception(
            f'Error while writing audit records to {table_name}'
        )
        raise