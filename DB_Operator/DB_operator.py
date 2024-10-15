import pyodbc
import json
import SQL_Queries


class ConnectDB:
    @staticmethod
    def connect_to_db(server, database, user, password):
        connectionString = f'''DRIVER={{ODBC Driver 17 for SQL Server}};
                               SERVER={server};
                               DATABASE={database};
                               UID={user};
                               PWD={password}'''

        try:
            conn = pyodbc.connect(connectionString)
            conn.autocommit = True
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            print('Successful Connection to DB!')
        finally:
            return conn


class MSSQLOperator:

    def __init__(self, connector: object):
        self.conn = connector

    def create_database(self, db_name, size=None, maxsize=None, filegrowth=None):
        SQL_COMMAND = SQL_Queries.create_database(db_name, size, maxsize, filegrowth)
        try:
            self.conn.execute(SQL_COMMAND)
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            return f'База данных {db_name} успешно создана!'

    def create_table(self, db_name, table_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        SQL_QUERY = sql_query(table_name)
        try:
            cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            return f'Таблица: {table_name} успешно создана!'

    def o_exist_query_1(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname, 'Rating': record.Rating}
                data_list.append(data_dict)
            return data_list

    def o_exist_query_2(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Year': record.Year}
                data_list.append(data_dict)
            return data_list

    def o_any_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'StudentName': record.StudentName, 'Phone': record.Phone}
                data_list.append(data_dict)
            return data_list

    def o_some_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'SubjectID': record.SubjectID, 'SubjectName': record.SubjectName}
                data_list.append(data_dict)
            return data_list

    def o_all_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'StudentID': record.StudentID, 'StudentName': record.StudentName}
                data_list.append(data_dict)
            return data_list

    def o_all_any_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'StudentName': record.StudentName, 'Email': record.Email}
                data_list.append(data_dict)
            return data_list

    def o_union_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname}
                data_list.append(data_dict)
            return data_list

    def o_union_all_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname}
                data_list.append(data_dict)
            return data_list

    def o_inner_join_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname}
                data_list.append(data_dict)
            return data_list

    def o_left_join_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname}
                data_list.append(data_dict)
            return data_list

    def o_right_join_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname}
                data_list.append(data_dict)
            return data_list

    def o_left_right_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'Name': record.Name, 'Surname': record.Surname}
                data_list.append(data_dict)
            return data_list

    def o_left_right_join_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'StudentName': record.StudentName, 'SubjectName': record.SubjectName,
                             'Score': record.Score}
                data_list.append(data_dict)
            return data_list

    def o_full_join_query(self, db_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {db_name}')
        data_list = []
        SQL_QUERY = sql_query
        try:
            result = cursor.execute(SQL_QUERY)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            records = result.fetchall()
            for record in records:
                data_dict = {'StudentName': record.StudentName, 'GroupName': record.GroupName}
                data_list.append(data_dict)
            return data_list

    def to_json(self, filename, data):
        with open(f'json_files/{filename}.json', 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            return f'Данные записаны в файл {filename}.json'

    def from_json(self, filename):
        with open(filename, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
