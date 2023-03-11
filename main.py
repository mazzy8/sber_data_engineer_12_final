#!/usr/bin/python3
import psycopg2
import os
import pandas as pd
from datetime import datetime
import logging


# настройка логирования
logging.basicConfig(level=logging.INFO, filename=f"./logs/{datetime.now().date()}.log",
                    format="%(asctime)s %(levelname)s %(message)s")

# глобальные переменные
STAGE_TABLES = ["DE12.buma_stg_transactions", "DE12.buma_stg_terminals",
                "DE12.buma_stg_passport_blacklist", "DE12.buma_stg_accounts",
                "DE12.buma_stg_cards", "DE12.buma_stg_clients"]


def clear_tables(cursor, tables):
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            logging.info(f"Очистка таблицы {table}")
        except Exception as e:
            logging.error(f"Ошибка очистки таблицы {table}: {e}")


def get_data_from_exel(path: str):
    if path and os.path.isfile(path):
        try:
            dataframe = pd.read_excel(path, index_col=None, header=0)
            return dataframe
        except Exception as e:
            logging.error(f"Ошибка работы с файлом {path}: {e}")
            return None
    else:
        return 'Empty file'


logging.info(f"Старт скрипта")

# Создание подключения к источнику
conn_src = psycopg2.connect(database="bank",
                            host="de-edu-db.chronosavant.ru",
                            user=os.getenv('user1'),
                            password=os.getenv('pass1'),
                            port="5432")

# Создание подключения к хранилищу
conn_dwh = psycopg2.connect(database="edu",
                            host="de-edu-db.chronosavant.ru",
                            user=os.getenv('user2'),
                            password=os.getenv('pass2'),
                            port="5432")

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()


logging.info(f"Запуск очистки таблиц в Stage:")
clear_tables(cursor_dwh, STAGE_TABLES)


logging.info(f"Запуск загрузки данных из файлов в Stage:")
logging.info(f"Загрузка из csv в Stage, de12.buma_stg_transaction")

df = pd.read_csv('transactions_01032021.txt', delimiter=';')
cursor_dwh.executemany("INSERT INTO de12.buma_stg_transactions(trans_id, trans_date, amount, card_num, oper_type,"
                       " oper_result, terminal) VALUES( %s, %s, %s, %s, %s, %s, %s)", df.values.tolist())

logging.info(f"Загрузка из xlsx в Stage, de12.buma_stg_terminals")
df = pd.read_excel('terminals_01032021.xlsx', index_col=None, header=0)
cursor_dwh.executemany("INSERT INTO de12.buma_stg_terminals(terminal_id, terminal_type, terminal_city, "
                       "terminal_address) VALUES( %s, %s, %s, %s)", df.values.tolist())

logging.info(f"Загрузка из xlsx в Stage, de12.buma_stg_passport_blacklist")
df = pd.read_excel('passport_blacklist_01032021.xlsx', index_col=None, header=0)
cursor_dwh.executemany("INSERT INTO de12.buma_stg_passport_blacklist"
                       "(date, passport) VALUES( %s, %s)", df.values.tolist())


logging.info(f"загрузка данных из источника в STAGE:")
cursor_src.execute("""select * from info.accounts;""")
for record in cursor_src:
    cursor_dwh.execute("INSERT INTO de12.buma_stg_accounts VALUES(%s, %s, %s, %s, %s)", record)

cursor_src.execute("""select * from info.cards;""")
for record in cursor_src:
    cursor_dwh.execute("INSERT INTO de12.buma_stg_cards VALUES(%s, %s, %s, %s)", record)

cursor_src.execute("""select * from info.clients;""")
for record in cursor_src:
    cursor_dwh.execute("INSERT INTO de12.buma_stg_clients VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", record)

conn_dwh.commit()

# загрузка из STAGE в DETAIL
# with open('./sql_scripts/stg_terminals_TO_dwh_dim_terminals.sql', 'r') as sql_file:
#     cursor_dwh.execute(sql_file.read())

# with open('./sql_scripts/stg_cards_TO_dwh_dim_cards.sql', 'r') as sql_file:
#     cursor_dwh.execute(sql_file.read())

# with open('./sql_scripts/stg_accounts_TO_dwh_dim_accounts.sql', 'r') as sql_file:
#     cursor_dwh.execute(sql_file.read())

# with open('./sql_scripts/stg_clients_TO_dwh_dim_clients.sql', 'r') as sql_file:
#     cursor_dwh.execute(sql_file.read())

# with open('./sql_scripts/stg_passport_blacklist_TO_dwh_fact_passport_blacklist.sql', 'r') as sql_file:
#     cursor_dwh.execute(sql_file.read())

# with open('./sql_scripts/stg_transactions_TO_dwh_fact_transactions.sql', 'r') as sql_file:
#     cursor_dwh.execute(sql_file.read())


# закрываем соединение
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()

logging.info(f"Завершение скрипта")