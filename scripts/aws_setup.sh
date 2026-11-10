#!/usr/bin/env bash
# =============================================================================
# AWS side of the pipeline: landing bucket + least-privilege IAM.
# Idempotent: safe to re-run. Requires AWS CLI v2 and admin-ish credentials
# (S3 + IAM) for the setup only.
#
#   Phase 1 — before Snowflake:
#     BUCKET=my-cdn-landing-123 REGION=eu-west-3 ./scripts/aws_setup.sh bootstrap
#
#   Phase 2 — after `desc integration cdn_s3_int` in Snowflake:
#     BUCKET=... REGION=... ./scripts/aws_setup.sh trust <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
#
#   Check:
#     BUCKET=... REGION=... ./scripts/aws_setup.sh verify
# =============================================================================
set -euo pipefail

: "${BUCKET:?Set BUCKET (globally unique bucket name)}"
: "${REGION:?Set REGION (same region as your Snowflake account, e.g. eu-west-3)}"
PREFIX="${PREFIX:-cdn_logs}"
WRITER_USER="${WRITER_USER:-cdn-airflow-writer}"
READER_ROLE="${READER_ROLE:-snowflake-cdn-landing}"
SECRETS_DIR="${SECRETS_DIR:-$(cd "$(dirname "$0")/.." && pwd)/secrets}"

export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
CALLER_ARN="$(aws sts get-caller-identity --query Arn --output text)"

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }

if [[ "$CALLER_ARN" == *":root" ]]; then
  echo "Refusing to run with root account credentials. Create an IAM admin user first." >&2
  exit 1
fi

writer_policy() {
  cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
     "Resource": "arn:aws:s3:::${BUCKET}/${PREFIX}/*"},
    {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::${BUCKET}",
     "Condition": {"StringLike": {"s3:prefix": ["${PREFIX}/*"]}}}
  ]
}
JSON
}

reader_policy() {
  cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion"],
     "Resource": "arn:aws:s3:::${BUCKET}/${PREFIX}/*"},
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
     "Resource": "arn:aws:s3:::${BUCKET}",
     "Condition": {"StringLike": {"s3:prefix": ["${PREFIX}/*"]}}}
  ]
}
JSON
}

trust_policy() {  # $1 principal ARN, $2 external id
  cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Principal": {"AWS": "$1"}, "Action": "sts:AssumeRole",
     "Condition": {"StringEquals": {"sts:ExternalId": "$2"}}}
  ]
}
JSON
}

bootstrap() {
  log "Account ${ACCOUNT_ID} as ${CALLER_ARN}, region ${REGION}"

  # ---------------------------------------------------------------- bucket
  if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    log "Bucket s3://${BUCKET} already exists"
  else
    log "Creating bucket s3://${BUCKET}"
    if [[ "$REGION" == "us-east-1" ]]; then
      aws s3api create-bucket --bucket "$BUCKET"
    else
      aws s3api create-bucket --bucket "$BUCKET" \
        --create-bucket-configuration "LocationConstraint=${REGION}"
    fi
  fi

  aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'
  aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration "{
    \"Rules\": [{\"ID\": \"cdn-logs-tiering\", \"Status\": \"Enabled\",
      \"Filter\": {\"Prefix\": \"${PREFIX}/\"},
      \"Transitions\": [{\"Days\": 30, \"StorageClass\": \"GLACIER_IR\"}],
      \"AbortIncompleteMultipartUpload\": {\"DaysAfterInitiation\": 1}}]}"
  log "Bucket hardened: public access blocked, SSE-S3, lifecycle to Glacier IR after 30 days"

  # ----------------------------------------------------- Airflow writer
  if ! aws iam get-user --user-name "$WRITER_USER" >/dev/null 2>&1; then
    log "Creating IAM user ${WRITER_USER}"
    aws iam create-user --user-name "$WRITER_USER" --tags Key=project,Value=cdn-elt >/dev/null
  fi
  aws iam put-user-policy --user-name "$WRITER_USER" --policy-name cdn-landing-write \
    --policy-document "$(writer_policy)"

  mkdir -p "$SECRETS_DIR" && chmod 700 "$SECRETS_DIR"
  local key_file="${SECRETS_DIR}/aws_airflow_writer.env"
  if [[ -f "$key_file" ]]; then
    log "Writer access key already stored in ${key_file}"
  else
    log "Creating access key for ${WRITER_USER}"
    aws iam create-access-key --user-name "$WRITER_USER" \
      --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text |
      awk '{printf "AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n", $1, $2}' >"$key_file"
    chmod 600 "$key_file"
    log "Saved to ${key_file} — copy these two lines into .env"
  fi

  # ----------------------------------------------------- Snowflake reader
  if ! aws iam get-role --role-name "$READER_ROLE" >/dev/null 2>&1; then
    log "Creating IAM role ${READER_ROLE} (placeholder trust until phase 2)"
    aws iam create-role --role-name "$READER_ROLE" \
      --assume-role-policy-document "$(trust_policy "arn:aws:iam::${ACCOUNT_ID}:root" "0000")" \
      --description "Assumed by Snowflake storage integration cdn_s3_int" \
      --tags Key=project,Value=cdn-elt >/dev/null
  fi
  aws iam put-role-policy --role-name "$READER_ROLE" --policy-name cdn-landing-read \
    --policy-document "$(reader_policy)"

  local role_arn
  role_arn="$(aws iam get-role --role-name "$READER_ROLE" --query Role.Arn --output text)"
  cat <<EOF

Done. Values for the next steps:
  snowflake/02_landing_s3.sql   <BUCKET>       = ${BUCKET}
                                <AWS_ROLE_ARN> = ${role_arn}
  .env                          CDN_LANDING_BUCKET=${BUCKET}
                                AWS_REGION=${REGION}
                                + the two lines in ${key_file}

Then run 02_landing_s3.sql, read STORAGE_AWS_IAM_USER_ARN and
STORAGE_AWS_EXTERNAL_ID from 'desc integration cdn_s3_int', and run:
  BUCKET=${BUCKET} REGION=${REGION} $0 trust <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
EOF
}

trust() {
  local sf_arn="${1:?Usage: $0 trust <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>}"
  local ext_id="${2:?Usage: $0 trust <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>}"
  log "Restricting ${READER_ROLE} trust to ${sf_arn}"
  aws iam update-assume-role-policy --role-name "$READER_ROLE" \
    --policy-document "$(trust_policy "$sf_arn" "$ext_id")"
  log "Done. In Snowflake: list @raw.cdn.cdn_logs_stage;"
}

verify() {
  local key_file="${SECRETS_DIR}/aws_airflow_writer.env"
  [[ -f "$key_file" ]] || { echo "Missing ${key_file}; run bootstrap first" >&2; exit 1; }
  local probe="${PREFIX}/_healthcheck/probe.txt"
  log "Writing s3://${BUCKET}/${probe} with the writer's own credentials"
  (
    # shellcheck disable=SC1090
    set -a && . "$key_file" && set +a
    unset AWS_SESSION_TOKEN AWS_PROFILE
    echo "ok $(date -u +%FT%TZ)" | aws s3 cp - "s3://${BUCKET}/${probe}" --only-show-errors
    aws s3 ls "s3://${BUCKET}/${PREFIX}/_healthcheck/"
    log "Writer must NOT be able to write outside ${PREFIX}/ (expect AccessDenied):"
    if echo x | aws s3 cp - "s3://${BUCKET}/outside.txt" --only-show-errors 2>/dev/null; then
      echo "WARNING: writer can write outside the prefix" >&2; exit 1
    else
      echo "  AccessDenied as expected"
    fi
  )
  aws s3 rm "s3://${BUCKET}/${probe}" --only-show-errors
  log "Writer policy verified"
}

case "${1:-}" in
  bootstrap) bootstrap ;;
  trust) shift; trust "$@" ;;
  verify) verify ;;
  *) sed -n '2,16p' "$0"; exit 1 ;;
esac
