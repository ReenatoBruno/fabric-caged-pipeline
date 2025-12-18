import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

def _build_relative_bucket_path(df: DataFrame) -> DataFrame:
    """   
    Adds a 'bucket_path' column to the DataFrame by extracting the relative 
    file path from the 'source_path' column.
    """
    
    df_with_bucket_path  = df.withColumn(
        'bucket_path', 
        f.expr("split(source_path, '/Files/', 2)[1]")
    )  
    return df_with_bucket_path

def _build_relative_lakehouse_path(df: DataFrame, 
                                   root_path: str) -> DataFrame:
    """
    Generates the logical lakehouse path by concatenating the root_path
    with the relative bucket_path.
    """

    df_with_lakehouse_path = df.withColumn(
        'lakehouse_path', 
        f.concat(
            f.lit(root_path + '/'), 
            f.split(f.col('source_path'), '/caged/').getItem(1)
        )
    )
    return df_with_lakehouse_path
    
def _select_meta_columns(df: DataFrame) -> DataFrame:
    """ 
    Select the final columns for the metadata
    """
    
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
        logging.info(
            'Starting metadata DataFrame build process'
        )

        logging.info(
            'Building relative bucket path from source_path.'
        )
        df_with_bucket_path = _build_relative_bucket_path(df=df)
        logging.info(
            'Relative bucket path built successfully'
        )
        
        logging.info(
            'Building relative lakehouse path (target destination)'
        )
        df_with_lakehouse_path = _build_relative_lakehouse_path(df=df_with_bucket_path, 
                                                                root_path=root_path)
        logging.info(
            'Relative lakehouse path built successfully'
        )

        logging.info(
            'Selecting final set of metadata columns.'
        )
        df_metadata = _select_meta_columns(df=df_with_lakehouse_path)
        logging.info(
            'Final metadata columns selected'
        )

        logging.info(
            'Metadata DataFrame build process completed'
        )

        return df_metadata
    except Exception as e:
        logging.error(
            f'FATAL ERROR in {build_metadata.__name__}: Metadata DataFrame building failed.',
            exc_info=True
        )
        raise