from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("raw_schema_name")

source_drivers_table = f"{catalog_name}.{schema_name}.race_drivers"
source_teams_table = f"{catalog_name}.{schema_name}.race_teams"
source_circuits_table = f"{catalog_name}.{schema_name}.race_circuits"
source_race_results_table = f"{catalog_name}.{schema_name}.race_results_unnested"

target_drivers_table = "dim_drivers"
target_teams_table = "dim_teams"
target_circuits_table = "dim_circuits"
target_race_results_table = "fact_race_results"

dp.create_streaming_table(
    name = target_drivers_table,
    comment = "Drivers Dimension Table stored in SCD Type-2",
    cluster_by_auto = True
)

dp.create_auto_cdc_flow(
  target = target_drivers_table,
  source = source_drivers_table,
  keys = ["driver_Id"],
  sequence_by = F.col("race_Date"),
  stored_as_scd_type = 2
)

dp.create_streaming_table(
    name = target_teams_table,
    comment = "Teams Dimension Table stored in SCD Type-2",
    cluster_by_auto = True
)

dp.create_auto_cdc_flow(
  target = target_teams_table,
  source = source_teams_table,
  keys = ["team_id"],
  sequence_by = F.col("race_Date"),
  stored_as_scd_type = 2
)

dp.create_streaming_table(
    target_circuits_table,
    comment = "Circuits Dimension Table stored in SCD Type-2",
    cluster_by_auto = False
)

dp.create_auto_cdc_flow(
  target = target_circuits_table,
  source = source_circuits_table,
  keys = ["circuit_Id"],
  sequence_by = F.col("race_Date"),
  stored_as_scd_type = 2
)

dp.create_streaming_table(
    target_race_results_table,
    comment = "Race results stored in denormalized manner",
    cluster_by_auto = True,
)

@dp.view
def race_results_source():
    return dp.read_stream(source_race_results_table).withColumnRenamed("load_date_time", "refreshed_at")

dp.create_auto_cdc_flow(
  target = target_race_results_table,
  source = "race_results_source",
  keys = ["race_Id", "driver_Id"],
  sequence_by = F.col("refreshed_at"),
  stored_as_scd_type = 1
)