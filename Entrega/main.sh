#!/bin/bash
# =============================================================================
# Script de configuración del Data Lake para datos de Steam Games
# Práctica 5 - Cloud Computing
# =============================================================================

# --- CONFIGURACIÓN INICIAL ---
export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-steam-games-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

echo "=========================================="
echo "Configuración del Data Lake - Steam Games"
echo "=========================================="
echo "Región: $AWS_REGION"
echo "Account ID: $ACCOUNT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Role ARN: $ROLE_ARN"
echo "=========================================="

# =============================================================================
# 1. CONFIGURACIÓN S3 - Estructura de carpetas del Data Lake
# =============================================================================
echo ""
echo ">>> 1. Creando estructura S3..."

# Crear el bucket
aws s3 mb s3://$BUCKET_NAME

# Crear carpetas (objetos vacíos con / al final)
# - raw/: Datos sin procesar
# - processed/: Datos procesados
# - scripts/: Scripts de ETL
# - config/: Archivos de configuración
# - errors/: Logs de errores de Firehose
aws s3api put-object --bucket $BUCKET_NAME --key raw/
aws s3api put-object --bucket $BUCKET_NAME --key raw/steam_games/
aws s3api put-object --bucket $BUCKET_NAME --key processed/
aws s3api put-object --bucket $BUCKET_NAME --key processed/games_by_year/
aws s3api put-object --bucket $BUCKET_NAME --key processed/games_by_genre/
aws s3api put-object --bucket $BUCKET_NAME --key config/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/
aws s3api put-object --bucket $BUCKET_NAME --key queries/
aws s3api put-object --bucket $BUCKET_NAME --key errors/

echo "✓ Estructura S3 creada"

# =============================================================================
# 2. KINESIS DATA STREAM - Para ingesta de datos en tiempo real
# =============================================================================
echo ""
echo ">>> 2. Creando Kinesis Data Stream..."

aws kinesis create-stream \
    --stream-name steam-games-stream \
    --shard-count 1

# Esperar a que el stream esté activo
echo "Esperando a que el stream esté activo..."
aws kinesis wait stream-exists --stream-name steam-games-stream

echo "✓ Kinesis Data Stream creado"

# =============================================================================
# 3. LAMBDA - Función de transformación para Firehose
# =============================================================================
echo ""
echo ">>> 3. Creando función Lambda para transformación..."

# Empaquetar la función Lambda
zip -j firehose.zip firehose.py

# Crear la función Lambda
aws lambda create-function \
    --function-name steam-firehose-transform \
    --runtime python3.12 \
    --role $ROLE_ARN \
    --handler firehose.lambda_handler \
    --zip-file fileb://firehose.zip \
    --timeout 60 \
    --memory-size 128

# Obtener el ARN de la Lambda
export LAMBDA_ARN=$(aws lambda get-function --function-name steam-firehose-transform --query 'Configuration.FunctionArn' --output text)

echo "Lambda ARN: $LAMBDA_ARN"
echo "✓ Función Lambda creada"

# =============================================================================
# 4. KINESIS FIREHOSE - Consumidor que transforma y almacena en S3
# =============================================================================
echo ""
echo ">>> 4. Creando Kinesis Firehose Delivery Stream..."

aws firehose create-delivery-stream \
    --delivery-stream-name steam-delivery-stream \
    --delivery-stream-type KinesisStreamAsSource \
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$AWS_REGION:$ACCOUNT_ID:stream/steam-games-stream,RoleARN=$ROLE_ARN" \
    --extended-s3-destination-configuration '{
        "BucketARN": "arn:aws:s3:::'"$BUCKET_NAME"'",
        "RoleARN": "'"$ROLE_ARN"'",
        "Prefix": "raw/steam_games/release_year=!{partitionKeyFromLambda:release_year}/",
        "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
        "BufferingHints": {
            "SizeInMBs": 64,
            "IntervalInSeconds": 60
        },
        "DynamicPartitioningConfiguration": {
            "Enabled": true,
            "RetryOptions": {
                "DurationInSeconds": 300
            }
        },
        "ProcessingConfiguration": {
            "Enabled": true,
            "Processors": [
                {
                    "Type": "Lambda",
                    "Parameters": [
                        {
                            "ParameterName": "LambdaArn",
                            "ParameterValue": "'"$LAMBDA_ARN"'"
                        },
                        {
                            "ParameterName": "BufferSizeInMBs",
                            "ParameterValue": "1"
                        },
                        {
                            "ParameterName": "BufferIntervalInSeconds",
                            "ParameterValue": "60"
                        }
                    ]
                }
            ]
        }
    }'

echo "Kinesis Firehose Delivery Stream creado"

# =============================================================================
# 5. AWS GLUE - Base de datos y Crawler
# =============================================================================
echo ""
echo ">>> 5. Configurando AWS Glue..."

# Crear base de datos en Glue Catalog
aws glue create-database \
    --database-input '{"Name":"steam_games_db"}'

# Crear Crawler para analizar los datos en S3
aws glue create-crawler \
    --name steam-games-crawler \
    --role $ROLE_ARN \
    --database-name steam_games_db \
    --targets '{"S3Targets": [{"Path": "s3://'"$BUCKET_NAME"'/raw/steam_games/"}]}'

echo "✓ Base de datos Glue y Crawler creados"

# =============================================================================
# 6. SUBIR SCRIPTS ETL A S3
# =============================================================================
echo ""
echo ">>> 6. Subiendo scripts ETL a S3..."

# Subir scripts de agregación
aws s3 cp energy_aggregation_daily.py s3://$BUCKET_NAME/scripts/steam_aggregation_by_year.py
aws s3 cp energy_aggregation_monthly.py s3://$BUCKET_NAME/scripts/steam_aggregation_by_genre.py

echo "✓ Scripts ETL subidos"

# =============================================================================
# 7. AWS GLUE JOBS - Trabajos ETL
# =============================================================================
echo ""
echo ">>> 7. Creando trabajos ETL en Glue..."

export DATABASE="steam_games_db"
export TABLE="steam_games"

# Job 1: Agregación por año de lanzamiento
aws glue create-job \
    --name steam-aggregation-by-year \
    --role $ROLE_ARN \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://'"$BUCKET_NAME"'/scripts/steam_aggregation_by_year.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--database": "'"$DATABASE"'",
        "--table": "'"$TABLE"'",
        "--output_path": "s3://'"$BUCKET_NAME"'/processed/games_by_year/",
        "--enable-continuous-cloudwatch-log": "true",
        "--spark-event-logs-path": "s3://'"$BUCKET_NAME"'/logs/"
    }' \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X"

# Job 2: Agregación por género
aws glue create-job \
    --name steam-aggregation-by-genre \
    --role $ROLE_ARN \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://'"$BUCKET_NAME"'/scripts/steam_aggregation_by_genre.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--database": "'"$DATABASE"'",
        "--table": "'"$TABLE"'",
        "--output_path": "s3://'"$BUCKET_NAME"'/processed/games_by_genre/",
        "--enable-continuous-cloudwatch-log": "true",
        "--spark-event-logs-path": "s3://'"$BUCKET_NAME"'/logs/"
    }' \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X"

echo "✓ Trabajos ETL creados"

echo ""
echo "=========================================="
echo "CONFIGURACIÓN COMPLETADA"
echo "=========================================="
echo ""
echo "Próximos pasos:"
echo "1. Ejecutar el productor de datos:"
echo "   uv run kinesis.py"
echo ""
echo "2. Esperar a que los datos lleguen a S3 (1-2 minutos)"
echo ""
echo "3. Ejecutar el crawler para detectar el esquema:"
echo "   aws glue start-crawler --name steam-games-crawler"
echo ""
echo "4. Verificar estado del crawler:"
echo "   aws glue get-crawler --name steam-games-crawler --query 'Crawler.State'"
echo ""
echo "5. Ejecutar los trabajos ETL:"
echo "   aws glue start-job-run --job-name steam-aggregation-by-year"
echo "   aws glue start-job-run --job-name steam-aggregation-by-genre"
echo ""
echo "6. Ver estado de los jobs:"
echo "   aws glue get-job-runs --job-name steam-aggregation-by-year --max-items 1"
echo "   aws glue get-job-runs --job-name steam-aggregation-by-genre --max-items 1"
echo "=========================================="
