from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

schema = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("timestamp", LongType()),
    StructField("bot", BooleanType()),
    StructField("namespace", IntegerType()),
    StructField("comment", StringType())
])

@dp.table(
    name="silver_wikimedia",
    comment="Parsed and structured Wikimedia events"
)
def silver_wikimedia():

    df = spark.read.table("wikimedia.wiki.bronze_wikimedia")

    parsed_df = df.withColumn(
        "json",
        from_json(col("body"), schema)
    )

    return parsed_df.select(
        col("json.id"),
        col("json.type"),
        col("json.title"),
        col("json.user"),
        col("json.timestamp"),
        col("json.bot"),
        col("json.namespace"),
        col("json.comment")
    )