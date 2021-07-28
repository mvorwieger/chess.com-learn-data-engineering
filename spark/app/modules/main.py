#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession

from analytics import country_distribution
from spark.jobs.chess_com_api import ChessComApi


def main():
    spark = SparkSession.builder.appName('chess.com recent statistics') \
        .config("spark.mongodb.input.uri",
                "mongodb://admin:password@products_mongodb:27017/products_database?authSource=admin") \
        .config("spark.mongodb.output.uri", "mongodb://products_mongodb:27017/products_database") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.collection", "products") \
        .getOrCreate()

    api = ChessComApi()
    games = api.fetch_games()
    df = spark.createDataFrame(games)
    df = df.filter(df.isTie == False)

    print(country_distribution(df).collect())

if __name__ == '__main__':
    main()
