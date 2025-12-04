export AWS_REGION="us-east-1"
export ACCOUNT_ID="992382705242"
export BUCKET_NAME="datalake-consumo-energetico-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)
echo "Usando bucket: ${BUCKET_NAME} y rol ${ROLE_ARN}"
aws s3 mb s3://${BUCKET_NAME}
aws s3api put-object --bucket ${BUCKET_NAME} --key raw/
aws s3api put-object --bucket ${BUCKET_NAME} --key processed/
aws s3api put-object --bucket ${BUCKET_NAME} --key config/
aws s3api put-object --bucket ${BUCKET_NAME} --key scripts/

aws kinesis create-stream --stream-name energy-stream --shard-count 1