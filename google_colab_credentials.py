import gspread
import os

# Fix 1: Corrected try-except logic
try:
    from elt import *
except ImportError:
    print("Module 'elt' not found. Please check your file paths.")
    # Fix 2: 'continue' can only be used inside a loop (for/while). 
    # If this is not in a loop, use 'pass' or handle the error.
    pass 

from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery, storage
from google.cloud import bigquery_storage_v1 as bq_storage
from google.oauth2.service_account import Credentials
from google.colab import auth

def get_gc_credential():
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
    return gc

# bot_params = {
#    "telegram": {
#        "error_colab": {
#            "telegram_token": "7956982873:AAGyWyo38bIyL7vmRlGwalXst28Kzixxxxx",
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

gc = get_gc_credential()
bq_client = None
client_params = {
    "google_client": gc,
    "bigquery_client": bq_client,
}

# Instantiate Classes
# Ensure these classes are defined above or imported.
extract = Extract(client_params)
transform = Transform(client_params)
load = Load(
   client_params,
   # telegram_token=bot_params['telegram']['error_colab']['telegram_token'],
   # chat_id=bot_params['telegram']['error_colab']['chat_id'],
)
