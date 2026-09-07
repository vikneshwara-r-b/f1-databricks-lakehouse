import os
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


@dp.table(
    name="raw_race",
    comment="Stored raw race details data sourced from F1 API"
)
def load_raw_race_details():
    volume_root_path = spark.conf.get("volume_root_path")
    volume_subdir_path = spark.conf.get("volume_subdir_path")
    volume_path = os.path.join(volume_root_path, volume_subdir_path)
    
    ddl_schema_str = "STRUCT<api: STRING, limit: BIGINT, offset: BIGINT, races: STRUCT<circuit: ARRAY<STRUCT<circuitId: STRING, circuitLength: STRING, circuitName: STRING, city: STRING, corners: BIGINT, country: STRING, fastestLapDriverId: STRING, fastestLapTeamId: STRING, fastestLapYear: BIGINT, firstParticipationYear: BIGINT, lapRecord: STRING, url: STRING>>, date: STRING, raceId: STRING, raceName: STRING, results: ARRAY<STRUCT<driver: STRUCT<birthday: STRING, driverId: STRING, name: STRING, nationality: STRING, number: BIGINT, shortName: STRING, surname: STRING, url: STRING>, fastLap: STRING, grid: STRING, points: BIGINT, position: STRING, retired: STRING, team: STRUCT<constructorsChampionships: BIGINT, driversChampionships: BIGINT, firstAppareance: BIGINT, nationality: STRING, teamId: STRING, teamName: STRING, url: STRING>, time: STRING>>, round: STRING, time: STRING, url: STRING>, season: BIGINT, total: BIGINT, url: STRING>"
    native_schema = StructType.fromDDL(ddl_schema_str)

    # 3. Read Auto Loader stream (removed conflicting options)
    final_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(native_schema)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "*.json")
        .option("recursiveFileLookup", "true")
        .load(volume_path)
        .withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn("load_date_time", F.current_timestamp())
    )

    return final_df

@dp.table(
    name="race_raw_quarantined",
    comment="Stores invalid or corrupt JSON records caught by Auto Loader rescued data column."
)
def load_quarantine_race_details():
    return (
        dp.read_stream("raw_race")
        .filter(F.col("_rescued_data").isNotNull())
        .withColumn("load_date_time", F.current_timestamp())
    )

@dp.table(
    name="race_results_nested",
    comment="race results data ready for flattening and downstream transformations."
)
def load_nested_race_data():
   race_data_nested_df = (
        dp.read_stream("raw_race")
        .filter(F.col("_rescued_data").isNull())
        .select(
        F.col("url").alias("api_url"),
        F.col("season").alias("season_year"),
        F.col("races.round").alias("round_no"),
        F.col("races.date").alias("round_date"),
        F.col("races.raceId").alias("race_id"),
        F.col("races.raceName").alias("race_name"),
        F.col("races.circuit").alias("circuit_nested"),
        F.col("races.results").alias("results_nested"),
        "source_file_name",
        "load_date_time"
        )
    )
   return race_data_nested_df