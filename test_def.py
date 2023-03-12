import os
import re
from datetime import datetime
import time


NAMES_FILES_FOR_DOWNLOAD = ['passport_blacklist', 'terminal', 'transaction']


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


while check_and_get_files_to_download() is None:
    time_now = datetime.now().strftime('%H:%M')
    if time_now > '23:30' or time_now < '04:00':
        log_message = f"За отведенное время не обнаружены файлы с данными"
        break
    time.sleep(1800)
else:
    print('Здесь скрипт')
