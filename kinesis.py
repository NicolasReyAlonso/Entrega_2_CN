import boto3
from loguru import logger
import time
import json

STREAM_NAME = "energy-stream"
INPUT_FILE = "datos.json"

kinesis = boto3.client('kinesis')

def load_data(path: str):
    with open(path, 'r') as f:
        return json.load(f)
def run_procedure():
    data = load_data(INPUT_FILE)
    records_sent = 0
    series_list = data.get('included', [])
    logger.info(f"Iniciando transmision de: {STREAM_NAME}")    
    for serie in series_list:
        tipo_demanda = serie['attributes']['title']
        valores = serie['attributes']['values']
        for registro in valores:
            payload = {
                "tipo": tipo_demanda,
                "valor": registro["value"],
                "timestamp_origen": registro["datetime"],
                "porcentaje": registro['percentage'],
            }
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=tipo_demanda
            )
            records_sent += 1
            logger.info(f"Enviado registro {records_sent}: data with{response=}")
            time.sleep(0.01)  # Simula un retardo entre envíos
if __name__ == "__main__":
    run_procedure()