import json
import base64
import datetime

def lambda_handler(event, context):
    """
    Lambda para Kinesis Firehose: transforma y enriquece datos de juegos de Steam.
    - Añade timestamp de procesamiento
    - Filtra juegos gratuitos (precio = 0) marcándolos
    - Enriquece con categoría de precio
    - Crea partición por año de lanzamiento
    """
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        data_json = json.loads(payload)
        
        # Añadir timestamp de procesamiento
        processing_time = datetime.datetime.now(datetime.timezone.utc)
        data_json['processing_timestamp'] = processing_time.isoformat()
        
        # Enriquecimiento: categoría de precio
        price = float(data_json.get('price', 0))
        if price == 0:
            data_json['price_category'] = 'Free'
        elif price < 10:
            data_json['price_category'] = 'Budget'
        elif price < 30:
            data_json['price_category'] = 'Standard'
        elif price < 60:
            data_json['price_category'] = 'Premium'
        else:
            data_json['price_category'] = 'Deluxe'
        
        # Enriquecimiento: popularidad basada en recomendaciones
        recommendations = int(data_json.get('recommendations', 0))
        if recommendations == 0:
            data_json['popularity'] = 'New'
        elif recommendations < 100:
            data_json['popularity'] = 'Low'
        elif recommendations < 1000:
            data_json['popularity'] = 'Medium'
        elif recommendations < 10000:
            data_json['popularity'] = 'High'
        else:
            data_json['popularity'] = 'Very High'
        
        # Crear clave de partición por año de lanzamiento
        release_year = data_json.get('release_year', 'unknown')
        if not release_year or release_year == '':
            release_year = 'unknown'
        
        # Eliminar release_year del JSON para evitar duplicados con la partición
        if 'release_year' in data_json:
            del data_json['release_year']
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((json.dumps(data_json) + '\n').encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'release_year': str(release_year)
                }
            }
        }
        output.append(output_record)
    
    return {'records': output}