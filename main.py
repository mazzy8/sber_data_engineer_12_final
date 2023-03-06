import psycopg2
import os
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, filename=f"pdf_log_{datetime.now().date()}.log",
                    format="%(asctime)s %(levelname)s %(message)s")


def get_data_from_exel(path: str, sheet: str):
    """
    :param path: file path
    :param sheet: name exel sheet
    :return: dataframe from exel sheet or error
    """
    if path and os.path.isfile(path):
        try:
            dataframe = pd.read_excel(path, sheet_name=sheet, index_col=None, header=0)
            return dataframe
        except Exception as error:
            return f'File error: {error}'
    else:
        return 'Empty file'


def write_data_to_exel(path: str, sheet: str, dataframe: pd.DataFrame):
    """
    :param path: file path
    :param sheet: exel sheet name
    :param dataframe: data to write
    :return: True or error
    """
    try:
        dataframe.to_excel(path, sheet_name=sheet, header=True, index=False)
        return True
    except Exception as error:
        return f'Work with file error: {error}'


def manipulation_with_data(mode: str):
    try:
        with psycopg2.connect(**PSQL_CONN) as connection:
            print(f'{datetime.now()}: Start script and open connection')
            connection.autocommit = False
            cursor = connection.cursor()

            print(f'{datetime.now()}: get data from exel(mode = sheet)')
            df = get_data_from_exel('medicine.xlsx', mode)

            print(f'{datetime.now()}: create temporary table')
            cursor.execute("""DROP TABLE IF EXISTS de12.buma_med_results_temp """)
            cursor.execute("""CREATE TABLE de12.buma_med_results_temp 
                           ("Код пациента" int, "Анализ" text, "Значение" text)""")

            print(f'{datetime.now()}: insert data in temporary table')
            cursor.executemany("""INSERT INTO de12.buma_med_results_temp("Код пациента", "Анализ", "Значение")
                               VALUES(%s, %s, %s)""", df.values.tolist())
            connection.commit()

            print(f'{datetime.now()}: create main table  de12.buma_med_results')
            cursor.execute("""DROP TABLE IF EXISTS de12.buma_med_results """)
            cursor.execute("""CREATE TABLE de12.buma_med_results 
                           ("Телефон" text, "Имя" text, "Название анализа" text, "Заключение" text)""")
            connection.commit()

            print(f'{datetime.now()}: send main request and write to main table')
            cursor.execute(MAIN_SQL_REQUEST)
            connection.commit()

            print(f'{datetime.now()}: get data from main table')
            if mode == 'hard':
                cursor.execute(HARD_SQL_REQUEST)
            else:
                cursor.execute(EASY_SQL_REQUEST)

            print(f'{datetime.now()}: create dataframe and write to exel: {mode}.xlsx')
            names = [x[0] for x in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=names)
            write_data_to_exel(f'{mode}.xlsx', mode, df)

            print(f'{datetime.now()}: clearing db')
            cursor.execute("""DROP TABLE de12.buma_med_results_temp""")
            if mode == 'easy':
                cursor.execute("""DROP TABLE de12.buma_med_results""")
            connection.commit()
            print(f'{datetime.now()}: End script and close connection')
    except Exception as error:
        print(f'{datetime.now()}: Error - {error}')

