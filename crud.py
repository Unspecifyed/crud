from pymongo import MongoClient

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, password):
        # Initialize connection variables
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 31157
        DB = 'AAC'
        COL = 'animals'
        
        # Initialize MongoClient and database/collection
        self.client = MongoClient(f'mongodb://{username}:{password}@{HOST}:{PORT}')
        self.db = self.client[DB]
        self.collection = self.db[COL]

    def create(self, data):
        if data:
            self.collection.insert_one(data)
            return True
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, query):
        cursor = self.collection.find(query)
        result = [document for document in cursor]
        return result

    def update(self, query, data):
        result = self.collection.update_many(query, {"$set": data})
        return result.modified_count

    def delete(self, query):
        result = self.collection.delete_many(query)
        return result.deleted_count

    def filter_by_outcome_type(self, outcome_type):
        query = {'outcome_type': outcome_type}
        return self.read(query)

    def filter_by_breed(self, breed):
        query = {'breed': breed}
        return self.read(query)

    def filter_by_location(self, location):
        query = {'location': location}
        return self.read(query)

    def get_all_breeds(self):
        pipeline = [
            {"$group": {"_id": "$breed", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        return list(self.collection.aggregate(pipeline))
