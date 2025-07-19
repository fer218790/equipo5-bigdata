import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
 
# Cargar variables de entorno
load_dotenv()
 
MONGO_URI = os.getenv("MONGO_URI")
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
SYMBOL = "NVDA"
 
# Conectar con MongoDB
client = MongoClient(MONGO_URI)
db = client["finanzas_ETL"]
 
# Borrar todas las colecciones previas (opcional pero solicitado)
for name in db.list_collection_names():
    db[name].drop()
    print(f"🗑 Eliminada colección: {name}")
 
# Configuración de las APIs de Alpha Vantage
api_config = {
    "intraday_5min": {
        "url": f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=5min&apikey={API_KEY}",
        "key": "Time Series (5min)",
        "collection": "NVDA_intraday_5min"
    },
    "daily": {
        "url": f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&outputsize=full&apikey={API_KEY}",
        "key": "Time Series (Daily)",
        "collection": "NVDA_daily"
    },
    "weekly": {
        "url": f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={SYMBOL}&apikey={API_KEY}",
        "key": "Weekly Adjusted Time Series",
        "collection": "NVDA_weekly"
    },
    "monthly": {
        "url": f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={SYMBOL}&apikey={API_KEY}",
        "key": "Monthly Time Series",
        "collection": "NVDA_monthly"
    }
}
 
# Función para limpiar y convertir datos a tipos numéricos
def clean_record(date_str, values):
    mapping = {
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. adjusted close": "adjusted_close",
        "6. volume": "volume",
        "7. dividend amount": "dividend_amount",
        "8. split coefficient": "split_coefficient"
    }
 
    record = {
        "_id": date_str,
        "datetime": date_str
    }
 
    for key, new_key in mapping.items():
        if key in values:
            val = values[key]
            if new_key in ["volume"]:
                record[new_key] = int(float(val))
            else:
                record[new_key] = float(val)
 
    return record
 
# Obtener y guardar los datos
def fetch_and_store_data():
    for name, config in api_config.items():
        print(f"\n🔄 Obteniendo datos: {name}")
        response = requests.get(config["url"])
        data = response.json()
 
        if config["key"] not in data:
            print(f"❌ Error: No se encontró la clave '{config['key']}' en la respuesta.")
            continue
 
        timeseries = data[config["key"]]
        collection = db[config["collection"]]
 
        records = []
        for date_str, values in timeseries.items():
            record = clean_record(date_str, values)
            records.append(record)
 
        if records:
            collection.insert_many(records)
            print(f"✅ Guardados {len(records)} registros en '{config['collection']}'")
        else:
            print("⚠ No se insertaron registros.")
 
if _name_ == "_main_":
    fetch_and_store_data()