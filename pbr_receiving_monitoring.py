#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import smtplib
import urllib
import urllib.parse
import warnings
from email.message import EmailMessage
from sys import platform

import numpy as np
import pandas as pd
import pyodbc
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

start_time = datetime.datetime.now()
warnings.filterwarnings('ignore')

print('# Старт мониторинга получения ПБР #', datetime.datetime.now())
logging.info(f'Старт мониторинга получения ПБР.')

# Настройки для логера
if platform == 'linux' or platform == 'linux2':
    logging.basicConfig(filename=('/var/log/log-execute/'
                                  'log_journal_pbr_rec_monitoring.log.txt'),
                        level=logging.INFO,
                        format=('%(asctime)s - %(levelname)s - '
                                '%(funcName)s: %(lineno)d - %(message)s'))
elif platform == 'win32':
    logging.basicConfig(filename=(f'{pathlib.Path(__file__).parent.absolute()}'
                                  f'/log_journal_pbr_rec_monitoring.log.txt'),
                        level=logging.INFO,
                        format=('%(asctime)s - %(levelname)s - '
                                '%(funcName)s: %(lineno)d - %(message)s'))

# Загружаем yaml файл с настройками
with open(
    f'{pathlib.Path(__file__).parent.absolute()}/settings.yaml', 'r',
        encoding='utf-8') as yaml_file:
    settings = yaml.safe_load(yaml_file)
telegram_settings = pd.DataFrame(settings['telegram'])
pyodbc_settings = pd.DataFrame(settings['pyodbc_db'])
email_settings = pd.DataFrame(settings['email'])
logging.info(f'YAML файл с настройками успешно открыт.')


# Функция отправки excel с прогнозом через Email

def send_mail(sender, recipients, subject, text, file):
    host_email = str(email_settings.host[0])
    user_email = str(email_settings.user[0])
    port_email = int(email_settings.port[0])
    password_email = str(email_settings.password[0])
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg.set_content(text)

    with open(f'{pathlib.Path(__file__).parent.absolute()}/{file}', 'rb') as f:
        file_data = f.read()
    msg.add_attachment(file_data,
                       maintype='application',
                       subtype='xlsx',
                       filename=file,)

    with smtplib.SMTP_SSL(host_email, port_email) as s:
        s.login(user_email, password_email)
        s.send_message(msg)
        s.quit()


# Отправка прогноза по Email
def email_notification(message_to_receive):
    logging.info("Старт отправки отчета по Email")
    try:
        send_mail(str(email_settings.sender[0]),
                  email_settings.recipients[0],
                  f'Мониторинг получения ПБР '
                  f'{datetime.datetime.now()}',
                  message_to_receive,
                  None)
    except Exception as e:
        print(e)
        logging.error(f'pbr_receiving_monitoring: '
                      f'Ошибка отправки прогноза почтой: {e}')
        telegram(1, f'pbr_receiving_monitoring: '
                 f'Ошибка отправки прогноза почтой: {e}')
    logging.info("Финиш отправки отчета по Email")

# Функция отправки уведомлений в telegram на любое количество каналов
# (указать данные в yaml файле настроек)


def telegram(i, text):
    msg = urllib.parse.quote(str(text))
    bot_token = str(telegram_settings.bot_token[i])
    channel_id = str(telegram_settings.channel_id[i])

    retry_strategy = Retry(
        total=3,
        status_forcelist=[101, 429, 500, 502, 503, 504],
        method_whitelist=["GET", "POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    http.post(f'https://api.telegram.org/bot{bot_token}/'
              f'sendMessage?chat_id={channel_id}&text={msg}', timeout=10)

# Функция коннекта к базе Mysql
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def connection(i):
    server = str(pyodbc_settings.host[i])
    database = str(pyodbc_settings.database[i])
    username = str(pyodbc_settings.user[i])
    password = str(pyodbc_settings.password[i])
    return pyodbc.connect(f'DRIVER={{SQL Server}};'
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password}')


# dt = datetime.datetime.now().replace(microsecond=0, second=0, minute=0)
# datetime_header = f'{dt}\n'
text_nopbr = '📩🚫Не получены ПБР по станции:\n'
text_noconnect = '🌐🚫Нет связи с:\n'
logging.info(f'Старт анализа получения ПБР.')

for ses in range(len(pyodbc_settings.index)):
    last_pbr = datetime.datetime.now().hour + 1
    pbr_must_be = list(range(1, last_pbr))
    name_ses = pyodbc_settings.ses_name[ses]
    print(pbr_must_be)
    try:
        conn = connection(ses)
        cursor = conn.cursor()
        cursor.execute(f'SELECT*FROM m53500.dbo.PlanVersions WHERE '
                       f'day = DATEADD(HOUR, -3, DATEDIFF(d, 0, GETDATE())) '
                       f'AND dtWrite > '
                       f'DATEADD(HOUR, -4, DATEDIFF(d, 0, GETDATE()))')
        pbr_dataframe = pd.DataFrame(
            np.array(cursor.fetchall()),
            columns=['id', 'day', 'type', 'dtRecived', 'dtWrite'])
        conn.close()
        pbr_in_modes_db = list(pbr_dataframe['type'])
        print(pbr_in_modes_db)
        difference = list(set(pbr_must_be).difference(set(pbr_in_modes_db)))
        message = ', '.join(str(x) for x in difference)
        print(difference)
        if difference:
            text_nopbr += f'{pyodbc_settings.ses_name[ses]}: {message}\n'
    except Exception:
        logging.info(f'Нет связи с {pyodbc_settings.ses_name[ses]}')
        text_noconnect += f'{pyodbc_settings.ses_name[ses]}\n'
print(text_nopbr)
print(text_noconnect)
logging.info(f'Старт отправки сообщений в телеграм.')
# Если нет связи и ПБР не получен
if (text_noconnect != '🌐🚫Нет связи с:\n' and
        text_nopbr != '📩🚫Не получены ПБР по станции:\n'):
    telegram(1, f'{text_nopbr}{text_noconnect}')
    email_notification(f'{text_nopbr}{text_noconnect}')
# Если связь есть и ПБР не получен
elif (text_noconnect == '🌐🚫Нет связи с:\n' and
        text_nopbr != '📩🚫Не получены ПБР по станции:\n'):
    telegram(1, f'✅Подключение к станциям есть.\n{text_nopbr}')
    email_notification(f'✅Подключение к станциям есть.\n{text_nopbr}')
# Если нет связи по всем станциям
elif (text_noconnect != '🌐🚫Нет связи с:\n' and
        text_nopbr == '📩🚫Не получены ПБР по станции:\n'):
    telegram(1, f'❌Нет подключения ко всем станциям.')
    email_notification(f'❌Нет подключения ко всем станциям.')
# Если всё норм
elif (text_noconnect == '🌐🚫Нет связи с:\n' and
        text_nopbr == '📩🚫Не получены ПБР по станции:\n'):
    telegram(1, f'✅ПБРы по всем станциям получены и связь есть.')
    email_notification(f'✅ПБРы по всем станциям получены и связь есть.')
logging.info(f'Финиш отправки сообщений в телеграм.')

print('Финиш мониторинга получения ПБР 🏁')
print('Время выполнения:', datetime.datetime.now() - start_time)
logging.info('Финиш мониторинга получения ПБР 🏁')
telegram(1,
         f'Финиш мониторинга получения ПБР 🏁 '
         f'(∆={datetime.datetime.now() - start_time})')
