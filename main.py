from DBModule.DBService import DBService
from DataModule.CSVService import CSVService

db_service = DBService(database="your_way_db", user="postgres", password="123zhz", host="localhost", port="5432")

s = CSVService(db_service)

s.create_groups("train.csv")

#s.create_popular_publics("train.csv")

#s.clear_dataset("train.csv", "new_train.csv")

# for i in range(1, 100):
#     try:
#         group = db_service.get_group(i)

#         s.synthesize(
#             dataset_path='ready.csv',
#             dataset_with_synthesize_path='ready.csv',
#             tematics_publics_path='Тематические паблики.csv',
#             notematics_publics_path='мусорные паблики.csv',
#             group=group, count_data=20)
#     except:
#         continue

# s.synthesize_zero_group(
#     dataset_path='ready.csv',
#     dataset_with_synthesize_path='ready.csv',
#     notematics_publics_path='мусорные паблики.csv',
#     count_data=100
# )