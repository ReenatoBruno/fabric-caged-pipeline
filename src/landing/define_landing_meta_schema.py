from pyspark.sql import functions as f
from pyspark.sql.types import *

def define_landing_meta_schema():
    """  
    Define the schama for the landing meta table
    """

    return StructType([
        StructField('source_path', StringType(), False),
        StructField('source_modified_at', TimestampType(), False),
        StructField('source_size_mb', float(), False), 
        StructField('bucket_path', StringType(), False), 
        StructField('lakehouse_path', StringType(), False), 
        StructField('copied_at', StringType(), False)
    ])
