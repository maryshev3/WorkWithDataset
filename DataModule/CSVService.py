import pandas
from DBModule.DBService import DBService
from .data_functions import del_punctuation
from .data_functions import clear

#Класс также будет использоваться для добавления в CSV файл синтетических данных
class CSVService:
    __db_service__ = None

    def __init__(self, db_service):
        self.__db_service__ = db_service

    def create_popular_publics(self, dataset_path):
        #Читаем список групп из файла
        csv_groups = pandas.read_csv(dataset_path, delimiter=';')['groups']

        #Составляем частотный словарь
        frequence_dict = dict()

        for groups_str in csv_groups:
            groups = del_punctuation(str(groups_str).lower(), './\\!@#$%^&*()-+_?;\"\':`|<>[]').split(',')

            #очищаем record_groups от пустых
            i = 0
            while i < len(groups):
                if groups[i] == '' or groups[i] == ' ':
                    groups.pop(i)
                else:
                    i += 1

            for group in groups:
                group = del_punctuation(group, '')
                if group in frequence_dict:
                    frequence_dict[group] += 1
                else:
                    frequence_dict[group] = 1

        #Составляем список групп, которые будут удаляться из таблицы
        count_clearing_groups = int(len(frequence_dict) / 20)

        clearing_groups = list()

        while len(clearing_groups) < count_clearing_groups:
            current_max_frequence = max(frequence_dict.values())

            extended_groups = [group for group in frequence_dict if frequence_dict[group] == current_max_frequence]

            clearing_groups.extend(extended_groups)

            for group in extended_groups:
                del frequence_dict[group]

        while len(clearing_groups) > count_clearing_groups:
            clearing_groups.pop()

        #Осуществляем запись в БД
        self.__db_service__.create_popular_publics(clearing_groups)

    def create_groups(self, dataset_path):
        groups = pandas.read_csv(dataset_path, delimiter=';')[['old_code', 'code']]

        groups_dict = dict()

        for index, record in groups.iterrows():
            groups_dict[int(record['code'])] = int(record['old_code'])

        self.__db_service__.create_groups(groups_dict)

    def clear_dataset(self, dataset_path, cleared_dataset_path):
        csv_file = pandas.read_csv(dataset_path, delimiter=';')

        #Проводим очистку CSVFile
        for index, record in csv_file.iterrows():
            record_groups = del_punctuation(str(record['groups']).lower(), './\\!@#$%^&*()-+_?;\"\':`|<>[]').split(',')

            #очищаем record_groups от пустых
            i = 0
            while i < len(record_groups):
                if record_groups[i] == '' or record_groups[i] == ' ':
                    record_groups.pop(i)
                else:
                    record_groups[i] = del_punctuation(record_groups[i], '')
                    i += 1

            record_groups = clear(record_groups, self.__db_service__)

            group_str = ' '.join(record_groups)
            while '  ' in group_str:
                group_str = group_str.replace('  ', ' ')

            csv_file.at[index, 'groups'] = group_str

        csv_file.to_csv(cleared_dataset_path, index = False, sep=';')

    def synthesize(this, dataset_path, dataset_with_synthesize_path, tematics_publics_path, notematics_publics_path, group, count_data = 1):
        tematics_csv_file = pandas.read_csv(tematics_publics_path, delimiter=';')
        no_tematics_csv_file = pandas.read_csv(notematics_publics_path, delimiter=';')

        #Оставляем в тематическом DataFrame только записи, относящиеся к группе направлений group
        tematics_csv_file = tematics_csv_file[tematics_csv_file.old_code == group]

        #Формируем запись
        csv_file = pandas.read_csv(dataset_path, delimiter=';')

        for i in range(0, count_data):
            id_direction_code = 0
            #Определим id_direction_code (фактический порядковый номер группы направлений в текущей таблице)
            for index, record in csv_file.iterrows():
                if int(record['old_code']) == group:
                    id_direction_code = int(record['code'])
                    break
            if id_direction_code == 0:
                continue

            #Оставляем случайные сообщества
            TematicsCSVFileRand = tematics_csv_file.sample(n = 3)
            NoTematicsCSVFileRand = no_tematics_csv_file.sample(n = 10)
            
            GroupsList = TematicsCSVFileRand['groups'].to_list()
            GroupsList.extend(NoTematicsCSVFileRand['groups'].to_list())

            j = 0
            while j < len(GroupsList):
                GroupsList[j] = del_punctuation(GroupsList[j], './\\!@#$%^&*()-+_?;\"\':`|<>[]')
                j += 1

            csv_file.loc[len(csv_file.index)] = [len(csv_file) + 1, group, id_direction_code, '', del_punctuation(' '.join(GroupsList).lower(), './\\!@#$%^&*()-+_?;\"\':`|<>[]')]

        csv_file.to_csv(dataset_with_synthesize_path, index = False, sep=';')

    def synthesize_zero_group(this, dataset_path, dataset_with_synthesize_path, notematics_publics_path, count_data = 1):
        no_tematics_csv_file = pandas.read_csv(notematics_publics_path, delimiter=';')

        #Формируем запись
        csv_file = pandas.read_csv(dataset_path, delimiter=';')

        for i in range(0, count_data):
            id_direction_code = 0

            #Оставляем случайные сообщества
            NoTematicsCSVFileRand = no_tematics_csv_file.sample(n = 13)
            
            GroupsList = (NoTematicsCSVFileRand['groups'].to_list())

            j = 0
            while j < len(GroupsList):
                GroupsList[j] = del_punctuation(GroupsList[j], './\\!@#$%^&*()-+_?;\"\':`|<>[]')
                j += 1

            csv_file.loc[len(csv_file.index)] = [len(csv_file) + 1, 0, id_direction_code, '', del_punctuation(' '.join(GroupsList).lower(), './\\!@#$%^&*()-+_?;\"\':`|<>[]')]

        csv_file.to_csv(dataset_with_synthesize_path, index = False, sep=';')
