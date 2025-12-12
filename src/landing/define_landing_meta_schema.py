from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

def define_landing_meta_schema() -> StructType:
    """  
    Define the schama for the landing meta table
    """

    meta_schema = StructType([
        StructField('source_path', StringType(), False), 
        StructField('source_modified_at', TimestampType(), False), 
        StructField('source_size_mb', FloatType(), False), 
        StructField('bucket_path', StringType(), False), 
        StructField('lakehouse_path', StringType(), False)
    ])

    return meta_schema

