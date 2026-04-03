from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_unixtime, to_timestamp

@dp.view(
    name="silver_user_changes",
    comment="Source data for SCD Type 2 user dimension"
)
def silver_user_changes():

    df = spark.read.table("silver_wikimedia")

    df = df.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("timestamp")))
    )

    return df.select(
        col("user"),
        col("title").alias("latest_title"),
        col("bot").alias("is_bot"),
        col("event_time")
    )

dp.create_streaming_table(
    name="gold_user",
    comment="User dimension table with SCD Type 2 tracking"
)

dp.apply_changes(
    target="gold_user",
    source="silver_user_changes",
    keys=["user"],
    sequence_by=col("event_time"),
    stored_as_scd_type="2",
    ignore_null_updates=True
)