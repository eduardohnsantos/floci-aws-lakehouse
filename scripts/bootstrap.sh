#!/usr/bin/env bash
# Creates the local resources in Floci.
# UNTESTED SKELETON: run step by step and adjust to what your Floci version supports.
set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
ACCOUNT=000000000000
ROLE="arn:aws:iam::${ACCOUNT}:role/lakehouse-role"
LAMBDA_ENDPOINT="${LAMBDA_ENDPOINT_URL:-http://host.docker.internal:4566}"

echo "== S3 buckets (landing / bronze / silver / gold / quarantine / athena results)"
for b in lakehouse-landing lakehouse-bronze lakehouse-silver lakehouse-gold lakehouse-quarantine lakehouse-athena-results; do
  aws s3 mb "s3://$b" 2>/dev/null || echo "exists: $b"
done

echo "== IAM role"
aws iam create-role --role-name lakehouse-role \
  --assume-role-policy-document file://infra/trust-policy.json || true

echo "== Kinesis stream"
aws kinesis create-stream --stream-name anp-events --shard-count 1 || true

echo "== Firehose: Kinesis -> S3 bronze"
aws firehose create-delivery-stream --delivery-stream-name anp-to-bronze \
  --delivery-stream-type KinesisStreamAsSource \
  --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:${AWS_DEFAULT_REGION}:${ACCOUNT}:stream/anp-events,RoleARN=${ROLE}" \
  --extended-s3-destination-configuration "BucketARN=arn:aws:s3:::lakehouse-bronze,RoleARN=${ROLE},Prefix=anp/" || true

echo "== SNS topic for alerts"
aws sns create-topic --name pipeline-alerts || true

echo "== Lambdas: extract + transform (same package, different handlers)"
mkdir -p build
rm -f build/lambda.zip
(cd src && zip -qr ../build/lambda.zip lakehouse -x "*/__pycache__/*")
deploy_lambda() {
  aws lambda create-function --function-name "$1" \
    --runtime python3.12 --handler "$2" --role "$ROLE" \
    --zip-file fileb://build/lambda.zip --timeout "$3" \
    --environment "Variables={AWS_ENDPOINT_URL=${LAMBDA_ENDPOINT}}" || true
}
deploy_lambda lakehouse-extract lakehouse.extract.handler 300
deploy_lambda lakehouse-transform lakehouse.handler.handler 120

echo "== Step Functions state machine"
aws stepfunctions create-state-machine --name lakehouse-pipeline \
  --definition file://statemachine/pipeline.asl.json --role-arn "$ROLE" || true

echo "== EventBridge Scheduler: daily run"
aws scheduler create-schedule --name lakehouse-daily \
  --schedule-expression "rate(1 day)" \
  --flexible-time-window Mode=OFF \
  --target file://infra/schedule-target.json || true

echo "== Glue database"
aws glue create-database --database-input Name=lakehouse || true

echo "done"
