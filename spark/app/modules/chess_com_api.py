from time import sleep

import requests


class ChessComApi:
    opponentCache = {}
    countryCache = {}

    def fetch_games(self, username: str, month: str, year: str) -> list:
        # transform "1" to "01", but keep "12" as "12" and not "012"
        month = "0" + month if len(month) < 1 else month

        return requests.get(
            "https://api.chess.com/pub/player/" + username + "/games/" + str(year) + "/" + month
        ).json()["games"]

    def fetch_opponent(self, opponentId):
        if opponentId in self.opponentCache:
            return self.opponentCache[opponentId]
        opponent = requests.get(opponentId).json()
        self.opponentCache[opponentId] = opponent
        return opponent

    def fetch_country(self, countryId):
        if countryId in self.countryCache:
            return self.countryCache[countryId]
        country = requests.get(countryId).json()
        self.countryCache[countryId] = country
        return country
