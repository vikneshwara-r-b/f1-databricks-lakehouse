from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import os

@dp.table(name="raw_race_details")
def load_raw_race_details():
    volume_root_path = spark.conf.get("volume_root_path")
    volume_subdir_path = spark.conf.get("volume_subdir_path")
    volume_path = os.path.join(volume_root_path, volume_subdir_path)
    
    # 1. Enforce a strict text schema since we want to read it as a raw row string
    text_schema = StructType([
        StructField("raw_json", StringType(), True)
    ])
    
    input_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")       # Treat the entire row/file as text
        .option("wholetext", "true")               # Use true if a JSON file spans multiple lines
        .schema(text_schema)                       # Explicitly provide the text schema
        .load(volume_path)
    )
    
    # 2. Rename, capture metadata, and append timestamps safely
    final_df = (
        input_df.withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn("last_load_datetime", F.current_timestamp())
    )
    
    return final_df
