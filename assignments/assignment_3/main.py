from pprint import pprint 
from DbConnector import DbConnector


class GeolifeDB:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_collections(self,):
        # Create collections if not already existing
        if "User" not in self.db.list_collection_names():
            self.db.create_collection("User")
            print("Created collection: User")
        if "Activity" not in self.db.list_collection_names():
            self.db.create_collection("Activity")
            print("Created collection: Activity")
        if "Trackpoint" not in self.db.list_collection_names():
            self.db.create_collection("Trackpoint")
            print("Created collection: Trackpoint")

    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    geolifeDB = None
    try:
        geolifeDB = GeolifeDB()
        geolifeDB.create_collections()

        #geolifeDB.show_coll()
        #geolifeDB.insert_documents(collection_name="Person")
        #geolifeDB.fetch_documents(collection_name="Person")
        geolifeDB.drop_coll(collection_name='User')
        geolifeDB.drop_coll(collection_name='Activity')
        geolifeDB.drop_coll(collection_name='Trackpoint')
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if geolifeDB:
            geolifeDB.connection.close_connection()


if __name__ == '__main__':
    main()
