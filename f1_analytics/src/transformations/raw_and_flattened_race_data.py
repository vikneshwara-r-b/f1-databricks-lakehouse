from pyspark import pipelines as dp
from pyspark.sql import functions as F
import os


@dp.table(name="raw_race_details")
def load_bronze_table():
    volume_root_path = spark.conf.get("volume_root_path")
    volume_subdir_path = spark.conf.get("volume_subdir_path")
    volume_path = os.path.join(volume_root_path, volume_subdir_path)
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(volume_path)
    )