import tweepy
import time
import orjson
import json
from pathlib import Path


class Mystream(tweepy.StreamingClient):
    directory = "/home/ubuntu/tweepy_cashtags/cashtag_output/"
    filename = "none"

    def on_connect(self):
        print("Connected")
        if self.filename == "none":
            now = time.gmtime()
            self.filename = str(now[0]) + "_" + str(now[1]) + "_" + \
                str(now[2]) + "_" + str(now[3]) + "_" + str(now[4]) + ".json"
            self.f = open(self.directory+self.filename, "a")
            self.f.write("{\"tweets\":[")

    def on_data(self, raw_data):
        if Path(self.directory+self.filename).stat().st_size >= 50000000:
            self.f.write("{}]}")
            self.f.close()
            self.filename = "none"
        if self.filename == "none":
            now = time.gmtime()
            self.filename = str(now[0]) + "_" + str(now[1]) + "_" + \
                str(now[2]) + "_" + str(now[3]) + "_" + str(now[4]) + ".json"
            self.f = open(self.directory+self.filename, "a")
            self.f.write("{\"tweets\":[")

        raw_data = orjson.loads(raw_data)
        json.dump(raw_data, self.f)
        self.f.write(",")

    def on_disconnect(self):
        self.f.write("{}]}")
        return super().on_disconnect()
