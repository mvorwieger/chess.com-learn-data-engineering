class Transform:
    def white_won(self, game):
        return game["white"]["result"] == "win"

    def black_won(self, game):
        return game["black"]["result"] == "win"

    def transform(self, games) -> list:
        for game in games:
            isWhite = game["white"]["username"] == "mivo09"
            opponentId = game["white"]["@id"] if not isWhite else game["black"]["@id"]
            o = self.fetch_opponent(opponentId)
            countryObj = self.fetch_country(o["country"])
            game["pgn"] = None
            del game["pgn"]
            opponent = {}
            opponent["@id"] = o["@id"]
            opponent["country"] = countryObj["name"]
            opponent["joined"] = o["joined"]
            opponent["countryCode"] = countryObj["code"]
            game["isTie"] = not self.white_won(game) and not self.black_won(game)
            game["won"] = self.white_won(game) and isWhite or not self.white_won(game) and not isWhite
            game["opponent"] = opponent
            print(game["opponent"])

        return games