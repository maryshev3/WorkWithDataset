from DBModule.DBService import DBService
from DataModule.CSVService import CSVService

db_service = DBService(database="your_way_db", user="postgres", password="123zhz", host="localhost", port="5432")

s = CSVService(db_service)

s.create_groups("train.csv")

s.create_popular_publics("train.csv")

s.clear_dataset("train.csv", "new_train.csv")