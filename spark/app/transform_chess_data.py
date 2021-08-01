import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_unixtime, when

mongodb_url = sys.argv[1]
month = sys.argv[2]
year = sys.argv[3]

spark = SparkSession.builder.getOrCreate()

def find_winner(game_df: DataFrame):
    return (
        when(game_df.white_result == "win", "white")
            .otherwise(
            when(game_df.black_result == "win", "black")
                .otherwise("tie")
        )
    )

countries_df_raw = (
    spark.read.format("mongo")
        .option("collection", "countries_extracted")
        .option("spark.mongodb.input.uri", mongodb_url)
        .load()
)

countries_no_duplicates = countries_df_raw.drop_duplicates(["@id"])

countries_df = (
    countries_no_duplicates.select(
        col("_id").alias("country_extracted_id"),
        col("@id").alias("country_id"),
        col("code"),
        col("name")
    )
)

players_df_raw = (
    spark.read.format("mongo")
        .option("collection", "players_extracted")
        .option("spark.mongodb.input.uri", mongodb_url)
        .load()
)

players_no_duplicates = players_df_raw.dropDuplicates(["@id"])

players_with_countries = players_no_duplicates.join(countries_df, players_no_duplicates.country == countries_df.country_id)

players_df = players_with_countries.select(
    col("_id").alias("player_extracted_id"),
    col("@id").alias("player_id"),
    countries_df.name.alias("country_name"),
    col("username"),
    col("code").alias("country_code")
)

games_df_raw = (
    spark.read.format("mongo")
        .option("collection", "games_extracted")
        .option("spark.mongodb.input.uri", mongodb_url)
        .load()
)

games_no_duplicates = games_df_raw.dropDuplicates(["url"])

games_df = (
    games_no_duplicates.select(
        col("_id").alias("extracted_id"),
        col("url").alias("match_id"),
        from_unixtime(games_df_raw.end_time).alias("date"),
        find_winner(games_df_raw).alias("winner"),
        col("black_@id").alias("black_player_id"),
        col("black_rating"),
        col("black_result"),
        col("white_@id").alias("white_player_id"),
        col("white_rating"),
        col("white_result")
    )
)

# we can overwrite here because we load all the data in memory anyways. in the futur we should probably choose appaned and just put some unique constraints on the table.
# apperently we can use shardKeys for this https://stackoverflow.com/questions/45505897/spark-scala-use-spark-mongo-connector-to-upsert
(
    games_df
        .write
        .format("mongo")
        .option("collection", "games_transformed")
        .option("spark.mongodb.output.uri", mongodb_url)
        .mode("overwrite")
        .save()
)

(
    players_df
        .write
        .format("mongo")
        .option("collection", "players_transformed")
        .option("spark.mongodb.output.uri", mongodb_url)
        .mode("overwrite")
        .save()
)

