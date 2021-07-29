import sys
from pyspark.sql import SparkSession
from modules import chess_com_api

mongodb_url = sys.argv[1]
month = sys.argv[2]
year = sys.argv[3]

spark = SparkSession.builder.getOrCreate()

print("######################################")
print("Starting to Transform Games")
print("######################################")

games_df = (
    spark.read.format("mongo")
        .option("spark.mongodb.input.uri", mongodb_url)
        .option("collection", "games_extracted")
        .load()
)

players_df = (
    spark.read.format("mongo")
        .option("spark.mongodb.input.uri", mongodb_url)
        .option("collection", "players_extracted")
        .load()
)

countries_df = (
    spark.read.format("mongo")
        .option("spark.mongodb.input.uri", mongodb_url)
        .option("collection", "countries_extracted")
        .load()
)
print("######################################")
print("Finished to Transform Games, Starint to save games now")
print("######################################")

(
    games_df.write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.uri", mongodb_url)
        .option("collection", "games")
        .save()
)

(
    players_df.write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.uri", mongodb_url)
        .option("collection", "players")
        .save()
)

(
    countries_df.write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.uri", mongodb_url)
        .option("collection", "countries")
        .save()
)

print("######################################")
print("Finished to Save Games")
print("######################################")
