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

    Output 'bucket_relative_path':
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


