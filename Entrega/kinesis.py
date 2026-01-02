import boto3
import json
import time
import csv
from loguru import logger

# CONFIGURACIÓN
STREAM_NAME = 'steam-games-stream'
REGION = 'us-east-1'
INPUT_FILE = 'dataset/a_steam_data_2021_2025.csv'
MAX_RECORDS = 2000  # Límite de registros a enviar

kinesis = boto3.client('kinesis', region_name=REGION)

def load_csv_data(file_path, max_records):
    """Carga datos del CSV y devuelve una lista de diccionarios (máx max_records)"""
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= max_records:
                break
            records.append(row)
    return records

def run_producer():
    records = load_csv_data(INPUT_FILE, MAX_RECORDS)
    records_sent = 0
    
    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}...")
    logger.info(f"Total de registros a enviar: {len(records)}")
    
    for registro in records:
        # Estructura del mensaje a enviar con datos de Steam
        payload = {
            'appid': registro.get('appid', ''),
            'name': registro.get('name', ''),
            'release_year': registro.get('release_year', ''),
            'release_date': registro.get('release_date', ''),
            'genres': registro.get('genres', ''),
            'categories': registro.get('categories', ''),
            'price': float(registro.get('price', 0)) if registro.get('price') else 0.0,
            'recommendations': int(registro.get('recommendations', 0)) if registro.get('recommendations') else 0,
            'developer': registro.get('developer', ''),
            'publisher': registro.get('publisher', '')
        }
        
        # Usar el género principal como clave de partición
        partition_key = registro.get('genres', 'Unknown').split(';')[0] if registro.get('genres') else 'Unknown'
        
        # Enviar a Kinesis
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey=partition_key
        )
        
        records_sent += 1
        
        # Log cada 1000 registros para no saturar
        if records_sent % 1000 == 0:
            logger.info(f"Registros enviados: {records_sent}/{len(records)}")
        
        # Pequeña pausa para simular streaming
        time.sleep(0.01)

    logger.info(f"Fin de la transmisión. Total registros enviados: {records_sent}")

if __name__ == '__main__':
    run_producer()