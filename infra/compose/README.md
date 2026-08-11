# Docker Compose

This directory defines the one-command local stack.

The initial Compose environment will include Kafka in KRaft mode, MinIO, PostgreSQL, Spark, Airflow, and implemented application containers. It owns local networks, volumes, ports, health checks, restart policies, and development-only defaults.

Compose should use the same application images and logical storage prefixes intended for AWS. Keep secrets in ignored environment files or a local secret mechanism, and provide safe sample configuration separately.
