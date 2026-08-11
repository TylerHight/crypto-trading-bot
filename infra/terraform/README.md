# Terraform

This directory provisions the AWS environment and its cost and security guardrails.

Expected resources include S3 buckets and lifecycle rules, Glue catalog objects, Athena workgroups, IAM roles and policies, networking, ECR/ECS or EC2 compute, RDS PostgreSQL, Secrets Manager entries, CloudWatch resources, EMR Serverless applications, optional MSK, and AWS Budgets alerts.

Prefer small composable modules and explicit environment roots. Remote state, generated plans, credentials, and account-specific variable files must not be committed. Disposable environments must support predictable `apply` and `destroy` workflows.
