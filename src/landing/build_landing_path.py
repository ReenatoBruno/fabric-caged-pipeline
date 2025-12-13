import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

def _extract_relative_path(df: DataFrame) -> DataFrame:
    """   
    Extract the relative bucket path within the OneLake
    
    Input'source_path': 
    abfss://b6d448cc-4a6a-4928-b3d7-b9e1e3be481a@onelake.dfs.fabric.microsoft.com/
    5b014b43-0a49-44a5-bc15-f25f51814631/Files/
    amzn-caged-bucket-renato/caged/2020/CAGEDMOV202001.txt

    Output 'bucket_path':
    amzn-caged-bucket-renato/caged/2020/CAGEDMOV202001.txt
    """
    logging.info(
        'Extracting relative bucket path'
    )
    try: 
        df_extracted  = df.withColumn(
            'bucket_path', 
            f.expr("split(source_path, '/Files/', 2)[1]")
        )
        logging.info (
            'Relative bucket path extracted successfully'
        )
        return df_extracted
    except Exception as e: 
        logging.exception(
            'An error occurred while extracting the bucket path'
        )
        raise e 

def _build_meta_path(df: DataFrame, 
                target_path: str) -> DataFrame:
    """
    Generates the logical lakehouse path by concatenating the target_path
    with the relative bucket_path.
      
    Target Path: LANDING_ROOT_PATH = 'Files/Landing/CAGED'
    """
    logging.info(
        'Generating logical lakehouse path based on target_path and bucket_path'
    )
    try:
        df_paths = df.withColumn(
            'lakehouse_path', 
            f.concat(f.lit(target_path + '/'), f.col('bucket_path'))
        )
        logging.info(
            'Logical lakehouse path column successfully generated'
        )
        return df_paths
    except Exception as e:
        logging.exception(
            'An error occurred while generating the logical lakehouse path'
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

def build_file_meta(df: DataFrame, 
                       target_path: str) -> DataFrame: 
    """ 
    Orchestrates the full landing-path build process and returns the final
    metadata DataFrame containing the columns:
    - source_path
    - source_modified_at
    - source_size_mb
    - bucket_path
    - lakehouse_path
    """

    df_extracted = _extract_relative_path(df=df)
      
    df_paths = _build_meta_path(df=df_extracted, 
                               target_path=target_path)
      
    df_meta_columns = _select_meta_columns(df=df_paths)

    return df_meta_columns