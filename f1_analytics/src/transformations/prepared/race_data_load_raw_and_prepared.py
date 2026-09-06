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
    
    ddl_schema_str = "STRUCT<api: STRING, championship: STRUCT<championshipId: STRING, championshipName: STRING, url: STRING, year: BIGINT>, race: ARRAY<STRUCT<championshipId: STRING, circuit: STRUCT<circuitId: STRING, circuitLength: STRING, circuitName: STRING, city: STRING, corners: BIGINT, country: STRING, fastestLapDriverId: STRING, fastestLapTeamId: STRING, fastestLapYear: BIGINT, firstParticipationYear: BIGINT, lapRecord: STRING, url: STRING>, fast_lap: STRUCT<fast_lap: STRING, fast_lap_driver_id: STRING, fast_lap_team_id: STRING>, laps: BIGINT, raceId: STRING, raceName: STRING, round: BIGINT, schedule: STRUCT<fp1: STRUCT<date: STRING, time: STRING>, fp2: STRUCT<date: STRING, time: STRING>, fp3: STRUCT<date: STRING, time: STRING>, qualy: STRUCT<date: STRING, time: STRING>, race: STRUCT<date: STRING, time: STRING>, sprintQualy: STRUCT<date: STRING, time: STRING>, sprintRace: STRUCT<date: STRING, time: STRING>>, teamWinner: STRUCT<constructorsChampionships: BIGINT, country: STRING, driversChampionships: BIGINT, firstAppearance: BIGINT, teamId: STRING, teamName: STRING, url: STRING>, url: STRING, winner: STRUCT<birthday: STRING, country: STRING, driverId: STRING, name: STRING, number: BIGINT, shortName: STRING, surname: STRING, url: STRING>>>, round: BIGINT, season: BIGINT, total: BIGINT, url: STRING>"
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
    name="quarantine_race",
    comment="Stores invalid or corrupt JSON records caught by Auto Loader rescued data column."
)
def load_quarantine_race_details():
    return (
        dp.read_stream("raw_race")
        .filter(F.col("_rescued_data").isNotNull())
        .withColumn("load_date_time", F.current_timestamp())
    )

@dp.table(
    name="race_data_nested",
    comment="race details data ready for flattening and downstream transformations."
)
def load_nested_race_data():
   race_data_nested_df = (
        dp.read_stream("raw_race")
        .filter(F.col("_rescued_data").isNull())
        .select(
        F.col("url").alias("api_url"),
        F.col("championship.championshipId").alias("championship_id"),
        F.col("championship.championshipName").alias("championship_name"),
        F.col("championship.year").alias("championship_year"),
        F.col("race").alias("race_data_nested"),
        F.col("season").alias("season_no"),
        F.col("round").alias("round_no"),
        "source_file_name",
        "load_date_time"
        )
    )
   return race_data_nested_df