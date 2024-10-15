import os
from dotenv import load_dotenv
from DB_operator import *

if __name__ == '__main__':
    load_dotenv()
    SERVER = os.getenv('MS_SQL_SERVER')
    DATABASE = os.getenv('MS_SQL_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')
    academy_db = 'Academy'
    student_evaluations_db = 'Student_Evaluations'

    my_conn = ConnectDB.connect_to_db(SERVER, DATABASE, USER, PASSWORD)
    my_db_operator = MSSQLOperator(my_conn)

    exist1_q = my_db_operator.o_exist_query_1(academy_db, SQL_Queries.exist_query_1())
    my_db_operator.to_json('exist_query_1', exist1_q)

    exist2_q = my_db_operator.o_exist_query_2(academy_db, SQL_Queries.exist_query_2())
    my_db_operator.to_json('exist_query_2', exist2_q)

    any_q = my_db_operator.o_any_query(student_evaluations_db, SQL_Queries.any_query())
    my_db_operator.to_json('any_query', any_q)

    all_q = my_db_operator.o_all_query(student_evaluations_db, SQL_Queries.all_query())
    my_db_operator.to_json('all_query', all_q)

    all_any_q = my_db_operator.o_all_any_query(student_evaluations_db, SQL_Queries.all_any_query())
    my_db_operator.to_json('all_any_query', all_any_q)

    union_all_q = my_db_operator.o_union_all_query(academy_db, SQL_Queries.union_all_query())
    my_db_operator.to_json('union_all_query', union_all_q)

    inner_join_q = my_db_operator.o_inner_join_query(academy_db, SQL_Queries.inner_join_query())
    my_db_operator.to_json('inner_join_query', inner_join_q)

    left_join_q = my_db_operator.o_left_join_query(academy_db, SQL_Queries.left_join_query())
    my_db_operator.to_json('left_join_query', left_join_q)

    right_join_q = my_db_operator.o_right_join_query(academy_db, SQL_Queries.right_join_query())
    my_db_operator.to_json('right_join_query', right_join_q)

    left_right_join_q = my_db_operator.o_left_right_join_query(student_evaluations_db,
                                                               SQL_Queries.left_right_join_query())
    my_db_operator.to_json('left_right_join_query', left_right_join_q)

    full_join_q = my_db_operator.o_full_join_query(student_evaluations_db, SQL_Queries.full_join_query())
    my_db_operator.to_json('full_join_query', full_join_q)
