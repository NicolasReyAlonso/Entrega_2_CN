export AWS_REGION="us-east-1"
export ACCOUNT_ID="992382705242"
export BUCKET_NAME="datalake-consumo-energetico-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)


aws firehose create-delivery-stream \
  --delivery-stream-name energy-delivery-stream \
  --delivery-stream-type KinesisStreamAsSource \
  --kinesis-stream-source-configuration "RoleARN=${ROLE_ARN},KinesisStreamARN=arn:aws:kinesis:${AWS_REGION}:${ACCOUNT_ID}:stream/energy-stream" \
  --extended-s3-destination-configuration "RoleARN=${ROLE_ARN},BucketARN=arn:aws:s3:::${BUCKET_NAME},Prefix=raw/energy_consumption_five_minutes/,ErrorOutputPrefix=errors/,BufferingHints={SizeInMBs=5,IntervalInSeconds=60}"


aws glue create-database --database-input '{"Name": "energy_database", "Description": "Database for energy consumption data"}'
aws glue create crawler \
    --name energy-crawler \
    --role ${ROLE_ARN} \
    --database-name energy_database \
    --targets "{\"S3Targets\": [{\"Path\": \"s3://${BUCKET_NAME}/raw/energy_consumption_five_minutes/\"}]}"