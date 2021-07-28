import sys
from pyspark.sql import SparkSession
from modules import chess_com_api

mongodb_url = sys.argv[1]
username = sys.argv[2]
month = sys.argv[3]
year = sys.argv[4]

spark = SparkSession.builder.getOrCreate()

print("######################################")
print("Starting to Extract Games")
print("######################################")

api = chess_com_api.ChessComApi()
games = api.fetch_games(username=username, year=year, month=month)
players = []
countries = []

for game in games:
    player_white = api.fetch_opponent(game["white"]["@id"])
    player_black = api.fetch_opponent(game["black"]["@id"])
    country = api.fetch_country(player_white["country"])

    players.append(player_black)
    players.append(player_white)
    countries.append(country)

    for key, value in game["black"].items():
        game["black_" + key] = value

    for key, value in game["white"].items():
        game["white_" + key] = value

    game["fetch_date_month"] = month
    game["fetch_date_year"] = year

    game.pop("pgn", None)
    game.pop("white", None)
    game.pop("black", None)

print("######################################")
print("Starting to Save Games")
print("######################################")

games_df = spark.createDataFrame(games)
(
    games_df.write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.uri", mongodb_url)
        .option("collection", "games_extracted")
        .save()
)

players_df = spark.createDataFrame(players)
(
    players_df.write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.uri", mongodb_url)
        .option("collection", "players_extracted")
        .save()
)

countries_df = spark.createDataFrame(countries)
(
    countries_df.write
        .format("mongo")
        .mode("append")
        .option("spark.mongodb.output.uri", mongodb_url)
        .option("collection", "countries_extracted")
        .save()
)

print("######################################")
print("Finished to Save Games")
print("######################################")
