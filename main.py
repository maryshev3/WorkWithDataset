from DBModule.DBService import DBService
from DataModule.CSVService import CSVService

db_service = DBService(database="your_way_db_", user="postgres", password="123zhz", host="localhost", port="5432")

s = CSVService(db_service)

s.create_groups("NEW_TRAIN.csv")

s.create_popular_publics("NEW_TRAIN.csv")

s.clear_dataset("NEW_TRAIN.csv", "ready.csv")

for i in range(1, 100):
    try:
        group = db_service.get_group(i)

        s.synthesize(
            dataset_path='ready.csv',
            dataset_with_synthesize_path='ready.csv',
            tematics_publics_path='Тематические паблики.csv',
            notematics_publics_path='мусорные паблики.csv',
            group=group, count_data=20)
    except:
        continue