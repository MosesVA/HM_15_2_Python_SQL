def create_database_default(name):
    COMMAND = fr'CREATE DATABASE {name}'
    return COMMAND


def create_database(name, size, maxsize, filegrowth):
    COMMAND = fr'''
    CREATE DATABASE {name}
    ON
    (
    NAME = {name}Database_data,
    FILENAME = "C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\{name}Database_data.mdf",
    SIZE = {size},
    MAXSIZE = {maxsize},
    FILEGROWTH = {filegrowth}
    )
    LOG ON
    (
    NAME = {name}Database_log,
    FILENAME = "C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\{name}Database_data.ldf",
    SIZE = {size},
    MAXSIZE = {str(int(maxsize) * 0.1)},
    FILEGROWTH = {filegrowth}
    )
    '''
    return COMMAND


def create_employers(table_name):
    QUERY = fr'''CREATE TABLE {table_name}
                 (employer_id int PRIMARY KEY,
                 employer_name nvarchar(100),
                 employer_url nvarchar(200)'''
    return QUERY


def create_vacancies(table_name):
    QUERY = fr'''CREATE TABLE {table_name}
                 (vacancy_id int PRIMARY KEY,
                 vacancy_name nvarchar(100),
                 vacancy_url nvarchar(200),
                 vacancy_salary_from int,
                 vacancy_salary_to int,
                 employer_id int
                 REFERENCES employers(employer_id));'''
    return QUERY


def exist_query_1():
    QUERY = fr'''SELECT Name, Surname, Rating
                 FROM Students
                 WHERE EXISTS(SELECT * FROM GroupsStudents
                                       WHERE GroupsStudents.GroupId = Students.Id);'''
    return QUERY


def exist_query_2():
    QUERY = fr'''SELECT Name, Year
                 FROM Groups
                 WHERE EXISTS(SELECT * FROM Departments
                                       WHERE Departments.Id = Groups.DepartmentId and Departments.Name = 'Design');'''
    return QUERY


def any_query():
    QUERY = fr'''SELECT StudentName, Phone
                 FROM Students_info
                 WHERE StudentID = ANY(SELECT Score
                                       FROM Scores);'''
    return QUERY


def some_query():
    QUERY = fr'''SELECT SubjectID, SubjectName
                 FROM Subjects_info
                 WHERE SubjectID = SOME(SELECT Score
                                        FROM Scores);'''
    return QUERY


def all_query():
    QUERY = fr'''SELECT StudentID, StudentName
                 FROM Students_info
                 WHERE StudentID = ALL(SELECT StudentID
                                       FROM Scores
                                       WHERE Score = 4 AND SubjectID = 7);'''
    return QUERY


def all_any_query():
    QUERY = fr'''SELECT StudentName, Email
                 FROM Students_info
                 WHERE StudentID = ALL(SELECT StudentID
                                       FROM Scores
                                       WHERE Score = 4 AND SubjectID = 7)
                 AND StudentID = ANY(SELECT StudentID
                                     FROM Scores
                                     WHERE Score = 5 AND SubjectID = 7);'''
    return QUERY


def union_query():
    QUERY = fr'''SELECT Name, Surname
                 FROM Curators

                 UNION

                 SELECT Name, Surname
                 FROM Teachers;'''
    return QUERY


def union_all_query():
    QUERY = fr'''SELECT Name, Surname
                 FROM Curators

                 UNION ALL

                 SELECT Name, Surname
                 FROM Teachers;'''
    return QUERY


def inner_join_query():
    QUERY = fr'''SELECT C.Name, C.Surname
                 FROM Curators AS C
                 INNER JOIN Teachers AS T ON C.Name = T.Name;'''
    return QUERY


def left_join_query():
    QUERY = fr'''SELECT T.Name, T.Surname
                 FROM Teachers AS T
                 LEFT JOIN Curators AS C ON T.Id = C.Id;'''
    return QUERY


def right_join_query():
    QUERY = fr'''SELECT T.Name, T.Surname
                 FROM Teachers AS T
                 RIGHT JOIN Curators AS C ON T.Id = C.Id;'''
    return QUERY


def left_right_join_query():
    QUERY = fr'''SELECT ST.StudentName, SU.SubjectName, SC.Score
                 FROM Students_info AS ST LEFT JOIN Scores AS SC
                 ON SC.StudentID = ST.StudentID  RIGHT JOIN Subjects_info AS SU
                 ON SU.SubjectID = SC.SubjectID
                 ORDER BY ST.StudentName;'''
    return QUERY


def full_join_query():
    QUERY = fr'''SELECT ST.StudentName, G.GroupName
                 FROM Students_info AS ST FULL JOIN Groups_info AS G
                 ON ST.GroupID = G.GroupID;'''
    return QUERY
