from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def create_spark_session():
    spark = SparkSession.builder.appName("dataproc-udacity").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    song_data = f"{input_data}/song-data/*/*/*/*.json"
    df = spark.read.json(song_data)
    print("Read song_data from GCS")

    songs_table = df.select(
        "song_id", "title", "artist_id", "year", "duration"
    ).dropDuplicates()
    songs_table.write.partitionBy("artist_id", "year").mode("overwrite").parquet(
        f"{output_data}/songs_table/"
    )
    print("Writing songs_table to parquet")

    artists_table = df.selectExpr(
        "artist_id",
        "artist_name as name",
        "artist_location as location",
        "artist_latitude as latitude",
        "artist_longitude as longitude",
    ).dropDuplicates()
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists_table/")
    print("Writing artists_table to parquet")


def process_log_data(spark, input_data, output_data):
    logs_data = f"{input_data}/log-data/*.json"
    df = spark.read.json(logs_data)
    print("Reading log_data from GCS")

    df = df.filter(df.page == "NextSong")

    users_table = df.selectExpr(
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level",
    ).dropDuplicates()
    users_table.write.mode("overwrite").parquet(f"{output_data}/users_table/")
    print("Writing users_table to parquet")

    df = df.withColumn("start_time", F.from_unixtime(F.col("ts") / 1000))
    time_table = (
        df.select("ts", "start_time")
        .withColumn("year", F.year("start_time"))
        .withColumn("month", F.month("start_time"))
        .withColumn("week", F.weekofyear("start_time"))
        .withColumn("weekday", F.dayofweek("start_time"))
        .withColumn("day", F.dayofmonth("start_time"))
        .withColumn("hour", F.hour("start_time"))
        .dropDuplicates()
    )

    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"{output_data}/time_table"
    )
    print("Writing time_table to parquet")

    song_data = f"{input_data}/song-data/A/A/A/*.json"
    song_dataset = spark.read.json(song_data)
    print("Read song_dataset from GCS")

    df.createOrReplaceTempView("log_dataset")
    song_dataset.createOrReplaceTempView("song_dataset")
    time_table.createOrReplaceTempView("time_table")

    songplays_table = spark.sql(
        """
        SELECT DISTINCT
            l.ts as ts,
            t.year,
            t.month,
            l.userId as user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId as session_id,
            s.artist_location,
            l.userAgent as user_agent
        FROM log_dataset l
        JOIN song_dataset s
            ON l.artist = s.artist_name
            AND l.song = s.title
            AND l.length = s.duration
        JOIN time_table t
            ON t.ts = l.ts
    """
    )
    print("SQL Query executed for songplays_table")

    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"{output_data}/songplays_table"
    )
    print("Writing songplays_table to parquet")


def main():
    spark = create_spark_session()
    input_data = "gs://udacitysongs/data/"
    output_data = "gs://udacitysongs/dataoutput/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
