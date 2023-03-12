#!/usr/bin/python3
import time
import psycopg2
import os
import re
import pandas as pd
from datetime import datetime
import logging
import requests


# настройка логирования
logging.basicConfig(level=logging.INFO, filename=f"./logs/{datetime.now().date()}.log",
                    format="%(asctime)s %(levelname)s %(message)s")
BOT_TOKEN = os.getenv('bot_token')

# списки таблиц и скриптов для циклов
STAGE_TABLES = ["DE12.buma_stg_transactions", "DE12.buma_stg_terminals",
                "DE12.buma_stg_passport_blacklist", "DE12.buma_stg_accounts",
                "DE12.buma_stg_cards", "DE12.buma_stg_clients"
                ]

SQL_SCRIPTS_TO_DWH = ['stg_terminals_TO_dwh_dim_terminals.sql', 'stg_cards_TO_dwh_dim_cards.sql',
                      'stg_accounts_TO_dwh_dim_accounts.sql', 'stg_clients_TO_dwh_dim_clients.sql',
                      'stg_passport_blacklist_TO_dwh_fact_passport_blacklist.sql',
                      'stg_transactions_TO_dwh_fact_transactions.sql'
                      ]

# параметры подключений к БД (логин и пароль в переменных окружения)
DB_BANK_SOURCE = {"dbname": "bank",
                  "host": "de-edu-db.chronosavant.ru",
                  "user": os.getenv('user1'),
                  "password": os.getenv('pass1'),
                  "port": "5432"
                  }

DB_EDU_DWH = {"dbname": "edu",
              "host": "de-edu-db.chronosavant.ru",
              "user": os.getenv('user2'),
              "password": os.getenv('pass2'),
              "port": "5432"
              }

# имена файлов для загрузки
NAMES_FILES_FOR_DOWNLOAD = ['passport_blacklist', 'terminal', 'transaction']


def clear_tables(cursor, tables: list):
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            logging.info(f"Таблица очищена {table}")
        except Exception as e:
            log_message = f"Ошибка очистки таблицы {table}: {e}"
            processing_error_message(log_message)


def download_to_dwh(cursor, scripts: list):
    for script in scripts:
        try:
            with open(f'./sql_scripts/{script}', 'r') as sql_file:
                cursor.execute(sql_file.read())
                logging.info(f"Выполнен скрипт {script}")
        except Exception as e:
            log_message = f"Ошибка выполнения скрипта {script}: {e}"
            processing_error_message(log_message)


def drop_to_archive(files: list):
    try:
        if not os.path.isdir("archive"):
            os.mkdir("archive")
        for file in files:
            os.replace(file, f"archive/{file + '.archive'}")
    except Exception as e:
        log_message = f"Ошибка работы скрипта архивирования: {e}"
        processing_error_message(log_message)


def get_data_from_exel(path: str):
    if path and os.path.isfile(path):
        try:
            dataframe = pd.read_excel(path, index_col=None, header=0)
            return dataframe
        except Exception as e:
            log_message = f"Ошибка работы с файлом {path}: {e}"
            processing_error_message(log_message)
            return None
    else:
        return 'Empty file'


def check_and_get_files_to_download():
    files = os.listdir("./")
    checked_files = []
    for file in files:
        for name in NAMES_FILES_FOR_DOWNLOAD:
            if name in file:
                checked_files.append(file)
    number_of_files = len(checked_files)
    if number_of_files == 3:
        date = re.search(r'\d{6,8}', checked_files[0])
        return checked_files if all([1 if date[0] in file else 0 for file in checked_files]) else None
    if number_of_files > 3:
        return None  # in process
    else:
        return None


def processing_error_message(message: str):
    logging.error(message)
    requests.post(url=f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
                  data={'chat_id': 397804346, 'text': message}).json()


logging.info(f"Старт скрипта")

while check_and_get_files_to_download() is None:
    logging.info(f"Файлы не обнаружены:")
    time_now = datetime.now().strftime('%H:%M')
    if time_now > '23:30' or time_now < '04:00':
        log_message = f"За отведенное время не обнаружены файлы с данными"
        processing_error_message(log_message)
        break
    time.sleep(60) # 1800
    logging.info(f"Повторная проверка файлов:")
else:
    logging.info(f"Файлы обнаружены")
    try:
        # Создание подключения к источнику
        conn_src = psycopg2.connect(**DB_BANK_SOURCE)

        # Создание подключения к хранилищу
        conn_dwh = psycopg2.connect(**DB_EDU_DWH)

        # Отключение автокоммита
        conn_src.autocommit = False
        conn_dwh.autocommit = False

        # Создание курсора
        cursor_src = conn_src.cursor()
        cursor_dwh = conn_dwh.cursor()

        logging.info(f"Очистка таблиц в Stage:")
        clear_tables(cursor_dwh, STAGE_TABLES)

        logging.info(f"Загрузка данных из файлов в Stage:")
        list_of_files = sorted(check_and_get_files_to_download())
        for file in list_of_files:
            logging.info(f"Загрузка из {file} в STAGE")
            if 'passport' in file:
                df = get_data_from_exel(file)
                cursor_dwh.executemany("INSERT INTO de12.buma_stg_passport_blacklist"
                                       "(date, passport) VALUES( %s, %s)", df.values.tolist())
            elif 'terminals' in file:
                df = get_data_from_exel(file)
                cursor_dwh.executemany("INSERT INTO de12.buma_stg_terminals(terminal_id, terminal_type, terminal_city, "
                                       "terminal_address) VALUES( %s, %s, %s, %s)", df.values.tolist())
            else:
                df = pd.read_csv(file, delimiter=';')
                cursor_dwh.executemany("INSERT INTO de12.buma_stg_transactions(trans_id, trans_date, amount, card_num, "
                                       "oper_type, oper_result, terminal) VALUES( %s, %s, %s, %s, %s, %s, %s)",
                                       df.values.tolist())

        logging.info(f"Загрузка данных из источника в STAGE:")
        cursor_src.execute("""select * from info.accounts;""")
        for record in cursor_src:
            cursor_dwh.execute("INSERT INTO de12.buma_stg_accounts VALUES(%s, %s, %s, %s, %s)", record)

        cursor_src.execute("""select * from info.cards;""")
        for record in cursor_src:
            cursor_dwh.execute("INSERT INTO de12.buma_stg_cards VALUES(%s, %s, %s, %s)", record)

        cursor_src.execute("""select * from info.clients;""")
        for record in cursor_src:
            cursor_dwh.execute("INSERT INTO de12.buma_stg_clients VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", record)

        logging.info(f"Загрузка данных из STAGE в DETAIL:")
        download_to_dwh(cursor_dwh, SQL_SCRIPTS_TO_DWH)

        conn_dwh.commit()
        # закрываем соединение
        cursor_src.close()
        cursor_dwh.close()
        conn_src.close()
        conn_dwh.close()
        # перемещаем файлы в архив
        logging.info(f"Перемещение файлов({list_of_files}) в архив:")
        drop_to_archive(list_of_files)
    except Exception as e:
        log_message = f"Ошибка исполнения скрипта: {e}"
        processing_error_message(log_message)

logging.info(f"Завершение скрипта")
