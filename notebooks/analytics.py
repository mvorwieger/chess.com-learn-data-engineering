from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col


def find_winner(game: DataFrame):
    return F.when(game.winner == "white", game.white_player_id).otherwise(
        F.when(game.winner == "black", game.black_player_id).otherwise(
            None
        )
    )

def country_distribution(games_df: DataFrame, username: str) -> DataFrame:
    return games_df.groupby(games_df.opponent.country.alias("country")).agg(
        F.count(games_df.opponent.country).alias("times_played"),
        F.avg(games_df.won.cast("double")).alias("win_percentage")).sort(col("times_played").desc())


def win_percentage_by_time_class_for_user(games_df: DataFrame, username: str) -> DataFrame:
    return games_df.groupby(games_df.time_class).agg(F.count(games_df.time_class).alias("times_played"),
                                                     F.avg((find_winner(games_df) == username).cast("double")).alias(
                                                         "win_percentage"))


def most_played_country_in_time_class(games_df: DataFrame) -> DataFrame:
    g = games_df.groupby(games_df.time_class, games_df.opponent.country.alias("country")).agg(
        F.count(games_df.time_class).alias("times_played"))

    return g.orderBy(games_df.time_class, g.times_played.desc()).groupBy(games_df.time_class).agg(
        F.max(g.times_played).alias("times_played"),
        F.first(g.country).alias("country"))


def times_played_against_nationality_in_time_class(games_df: DataFrame) -> DataFrame:
    return games_df.groupby(games_df.time_class, games_df.opponent.country.alias("country")).agg(
        F.count(games_df.time_control).alias("times_played"),
        F.avg(games_df.won.cast("double")).alias("win_percentage")).sort(
        games_df.time_class, col("times_played").desc())
