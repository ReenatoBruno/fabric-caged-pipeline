import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

def _build_relative_bucket_path(df: DataFrame) -> DataFrame:
    """   
    Build the relative bucket path within the OneLake
    
    Input'source_path': 
    abfss://b6d448cc-4a6a-4928-b3d7-b9e1e3be481a@onelake.dfs.fabric.microsoft.com/
    5b014b43-0a49-44a5-bc15-f25f51814631/Files/
    amzn-caged-bucket-renato/caged/2020/CAGEDMOV202001.txt

    Output 'bucket_path':
    amzn-caged-bucket-renato/caged/2020/CAGEDMOV202001.txt
    """
    logging.info(
        'Building relative bucket path...'
    )
    try: 
        df_with_bucket_path  = df.withColumn(
            'bucket_path', 
            f.expr("split(source_path, '/Files/', 2)[1]")
        )
        logging.info (
            'Relative bucket path built successfully'
        )
        return df_with_bucket_path
    except Exception as e: 
        logging.exception(
            'An error occurred while building the bucket path'
        )
        raise e 

def _build_relative_lakehouse_path(df: DataFrame, 
                                   root_path: str) -> DataFrame:
    """
    Generates the logical lakehouse path by concatenating the root_path
    with the relative bucket_path.
      
    LANDING_ROOT_PATH = 'Files/Landing/CAGED'
    """
    logging.info(
        'Building logical lakehouse path...'
    )
    try:
        df_with_lakehouse_path = df.withColumn(
            'lakehouse_path', 
            f.concat(f.lit(root_path + '/'), f.col('bucket_path'))
        )
        logging.info(
            'Logical lakehouse path built successfully'
        )
        return df_with_lakehouse_path
    except Exception as e:
        logging.exception(
            'An error occurred while building the logical lakehouse path'
        )
        raise e 
    
def _select_meta_columns(df: DataFrame) -> DataFrame:
    """ 
    Select the final columns for the metadata
    """
    
    df_meta_columns =  df.select('source_path',
                                 'source_modified_at',
                                 'source_size_mb',
                                 'bucket_path',
                                 'lakehouse_path')
    return df_meta_columns

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

    df_with_bucket_path = _build_relative_bucket_path(df=df)
      
    df_with_lakehouse_path = _build_relative_lakehouse_path(df=df_with_bucket_path, 
                                                            root_path=root_path)
      
    df_metadata = _select_meta_columns(df=df_with_lakehouse_path)

    return df_metadata