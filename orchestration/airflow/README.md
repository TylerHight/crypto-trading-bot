# Airflow

This directory owns the Airflow deployment-facing project: DAG discovery configuration, provider dependencies, plugins only when unavoidable, and local/AWS runtime documentation.

Airflow runs in Docker Compose locally and can use the same image on EC2 or ECS. Amazon MWAA is deferred. DAGs should call stable application or job interfaces rather than importing their private implementation details.
