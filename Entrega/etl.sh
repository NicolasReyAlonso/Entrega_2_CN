export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-steam-games-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)
export LAMBDA_ARN=$(aws lambda get-function --function-name steam-firehose-transform --query 'Configuration.FunctionArn' --output text)
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
