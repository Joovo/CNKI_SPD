from pymongo.mongo_client import MongoClient

from .settings import mongo_url

# departed module for using MongoDB as database
class MongoDB:

    def __init__(self):
        self._mongo_client = MongoClient(mongo_url)

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def connect(self):
        return self._mongo_client


    def __del__(self):
        self._mongo_client.close()
