import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

def _extract_relative_path(df: DataFrame) -> DataFrame:
    """   
    Extract the relative backet path within the Onelake
    
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
            'Extraction successfully completed'
        )
        return df_extracted
    except Exception as e: 
        logging.exception(
            'An error occurred while extracting the bucket path'
        )
        raise e 

def _build_path(df: DataFrame, 
                target_path: str) -> DataFrame:
    """
    Creates the full destination path for the Lakehouse/landing zone 
    by prepending the target_path to the relative bucket path
      
    Target Path: LANDING_ROOT_PATH = 'Files/Landing/CAGED'
    """
    logging.info(
        'Building path within the landing zone'
    )
    try:
        df_path = df.withColumn(
            'lakehouse_path', 
            f.concat(f.lit(target_path + '/'), f.col('bucket_path'))
        )
        logging.info(
            'landing zone path successfully created'
        )
        return df_path
    except Exception as e:
        logging.exception(
            'An error occurred while building the landing path'
        )
        raise e 
    
def _select_final_columns(df: DataFrame) -> DataFrame:
    """ 
    Select the final columns for the landing path
    """
    
    df_landing_path =  df.select('source_path',
                                 'source_modified_at',
                                 'surce_size_mb',
                                 'bucket_path',
                                 'lakehouse_path')
    return df_landing_path

def build_landing_path(df: DataFrame, 
                       target_path: str) -> DataFrame: 
    """ 
    Orchestrates the full landing-path build process and returns the final
    metadata DataFrame containing the columns:
    - source_modified_at
    - source_size_mb
    - bucket_path
    - lakehouse_path
    """

    df_extracted = _extract_relative_path(df=df)
      
    df_path = _build_path(df=df_extracted, 
                            target_path=target_path)
      
    df_landing_path =_select_final_columns(df=df_path)

    return df_landing_path