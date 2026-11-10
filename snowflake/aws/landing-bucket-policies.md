# AWS IAM for the landing bucket

Two principals touch the bucket, each with least privilege.

## 1. Airflow writer (IAM user or role used by the `aws_default` connection)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::<BUCKET>/cdn_logs/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<BUCKET>",
      "Condition": {"StringLike": {"s3:prefix": ["cdn_logs/*"]}}
    }
  ]
}
```

## 2. Snowflake reader (role assumed by the storage integration)

Permissions policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::<BUCKET>/cdn_logs/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::<BUCKET>",
      "Condition": {"StringLike": {"s3:prefix": ["cdn_logs/*"]}}
    }
  ]
}
```

Trust policy — fill with the values returned by `desc integration cdn_s3_int`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
      "Action": "sts:AssumeRole",
      "Condition": {"StringEquals": {"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"}}
    }
  ]
}
```

Recommended bucket settings: Block Public Access on, SSE-S3 encryption, and a
lifecycle rule moving `cdn_logs/` to Glacier Instant Retrieval after 30 days.
