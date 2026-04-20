import gspread
import os
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery, storage
from google.cloud import bigquery_storage_v1 as bq_storage
from google.oauth2.service_account import Credentials

def get_gc_credential():
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
    return gc

# bot_params = {
#    "telegram": {
#        "error_colab": {
#            "telegram_token": "7956982873:AAGyWyo38bIyL7vmRlGwalXst28KziWOxdg",
#            'chat_id': -1003206692629,
#            'topic': {'hourly': 2,
#                        'monthly': 4,
#                        'idea': 6,
#                        'daily': 8,
#                        'bot_conversation': 10,
#                        'weekly': 12,
#                        'bot test':14,
#                        'colab test':16,
#                        'airflow test':18,
#                        'operation':20
#                        },
#        },
#        "gui_danh_gia": {},
#    },
#    "gmail": {},
# }

bq_client = None
client_params = {
    "google_client": gc,
    "bigquery_client": bq_client,
}

# Call class to implement (assumes ETL/Extract/Load/Transform are imported elsewhere)
bot_notice = Bot(bot_params)
extract = Extract(client_params)
load = Load(
   client_params,
#    telegram_token=bot_params['telegram']['error_colab']['telegram_token'],
#    chat_id=bot_params['telegram']['error_colab']['chat_id'],
#    topic_id=bot_params['telegram']['error_colab']['topic']['operation']  # optional
)
transform = Transform(client_params)
# ext_load = ExtLoad(
#     client_params,
#     telegram_token=bot_params['telegram']['error_colab']['telegram_token'],
#     chat_id=bot_params['telegram']['error_colab']['chat_id'],
#     topic_id=bot_params['telegram']['error_colab']['topic']['operation']  # optional
# )
