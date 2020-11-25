from pymongo import MongoClient


class RepoService:
    def __init__(self, module):
        self.client = MongoClient(
            'mongodb://root:DataSus123*@45.79.41.194:27217/')
        self.collection = self.client.sus[module]

    def create_index_sus(self):
        return self.collection.create_index([("mes", 1), ("ano", 1), ("uf", 1)])
    
    def insert(self, obj):
        self.collection.insert_many(obj)
        # existing_document = self.collection.find_one({"id": obj['id']})
        # if not existing_document:
        #     self.collection.replace_one(obj, obj, upsert=True)
