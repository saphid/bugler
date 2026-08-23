#!/usr/bin/env bash
set -euo pipefail

REGION='us-east-1'
TABLE='bugler'
IAM_USER='bugler'
DRY_RUN=0
YES=0

usage() {
  printf 'Usage: %s [--region REGION] [--table TABLE] [--iam-user USER] [--yes] [--dry-run]\n' "$0"
}

while (($# > 0)); do
  case "$1" in
    --region) REGION=${2:?--region requires a value}; shift ;;
    --table) TABLE=${2:?--table requires a value}; shift ;;
    --iam-user) IAM_USER=${2:?--iam-user requires a value}; shift ;;
    --yes) YES=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if ((DRY_RUN)); then
  printf '%s\n' \
    'DRY RUN: aws sts get-caller-identity' \
    "DRY RUN: create or reuse DynamoDB table $TABLE in $REGION with pk/sk and received-index" \
    'DRY RUN: use provisioned 5 RCU and 5 WCU for the table and received-index' \
    "DRY RUN: create or reuse IAM user $IAM_USER and attach a least-privilege IAM policy for only this table and index" \
    'DRY RUN: create one access key only when the user has none, then show it once' \
    'DRY RUN: print BUGLER_STORAGE=dynamodb, BUGLER_DYNAMODB_TABLE, and AWS_REGION for your container' \
    'AWS compute is not provisioned. App Runner has no free tier, Lambda cannot host the core long-poll process, and ECS, Fargate, and Lightsail have plain costs.' \
    'The DynamoDB allowance is bounded. Usage beyond your account allowance can incur charges.'
  exit 0
fi

if ((YES == 0)); then
  printf '%s\n' '--yes is required because this command changes AWS resources' >&2
  exit 2
fi

command -v aws >/dev/null 2>&1 || { printf '%s\n' 'aws CLI is required' >&2; exit 1; }
aws sts get-caller-identity >/dev/null

if ! aws dynamodb describe-table --region "$REGION" --table-name "$TABLE" >/dev/null 2>&1; then
  aws dynamodb create-table \
    --region "$REGION" \
    --table-name "$TABLE" \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S AttributeName=gsi1pk,AttributeType=S AttributeName=gsi1sk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --global-secondary-indexes "IndexName=received-index,KeySchema=[{AttributeName=gsi1pk,KeyType=HASH},{AttributeName=gsi1sk,KeyType=RANGE}],Projection={ProjectionType=ALL},ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}" \
    >/dev/null
  aws dynamodb wait table-exists --region "$REGION" --table-name "$TABLE"
else
  INDEX_NAME=$(aws dynamodb describe-table --region "$REGION" --table-name "$TABLE" --query 'Table.GlobalSecondaryIndexes[0].IndexName' --output text)
  if [[ "$INDEX_NAME" != 'received-index' ]]; then
    printf 'Existing table %s does not have the required received-index GSI\n' "$TABLE" >&2
    exit 1
  fi
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
TABLE_ARN="arn:aws:dynamodb:$REGION:$ACCOUNT_ID:table/$TABLE"
POLICY=$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["dynamodb:BatchGetItem","dynamodb:BatchWriteItem","dynamodb:ConditionCheckItem","dynamodb:DeleteItem","dynamodb:DescribeTable","dynamodb:GetItem","dynamodb:PutItem","dynamodb:Query","dynamodb:TransactGetItems","dynamodb:TransactWriteItems","dynamodb:UpdateItem"],"Resource":["%s","%s/index/received-index"]}]}' "$TABLE_ARN" "$TABLE_ARN")

if ! aws iam get-user --user-name "$IAM_USER" >/dev/null 2>&1; then
  aws iam create-user --user-name "$IAM_USER" >/dev/null
fi
aws iam put-user-policy --user-name "$IAM_USER" --policy-name BuglerTableAccess --policy-document "$POLICY"

KEY_COUNT=$(aws iam list-access-keys --user-name "$IAM_USER" --query 'length(AccessKeyMetadata)' --output text)
if [[ "$KEY_COUNT" == '0' ]]; then
  IFS=$'\t' read -r ACCESS_KEY_ID SECRET_ACCESS_KEY < <(
    aws iam create-access-key --user-name "$IAM_USER" --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text
  )
  printf '%s\n' 'Access key created. Store these values now; the secret will not be shown again.'
  printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$ACCESS_KEY_ID" "$SECRET_ACCESS_KEY"
  unset ACCESS_KEY_ID SECRET_ACCESS_KEY
else
  printf '%s\n' 'The IAM user already has an access key. Reuse its securely stored credentials; AWS cannot show its secret again.'
fi

printf '\nContainer environment:\n'
printf 'BUGLER_STORAGE=dynamodb\nBUGLER_DYNAMODB_TABLE=%s\nAWS_REGION=%s\n' "$TABLE" "$REGION"
printf '%s\n' 'Also supply AWS credentials and BUGLER_BOOTSTRAP_ADMIN_TOKEN through your compute platform secret store.'
printf '%s\n' 'AWS compute was not provisioned. App Runner has no free tier, Lambda is only suitable for an ingest shim, and ECS, Fargate, and Lightsail can incur compute charges.'
