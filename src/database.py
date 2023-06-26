# a key value database

class Database():
    def __init__(self):
        self.storage = {}
    
    def set(self, key, value):
        self.storage.insert(key, value)
    
    def get(self, key):
        self.storage.get(key)

    def delete(self, key):
        self.storage.delete(key)