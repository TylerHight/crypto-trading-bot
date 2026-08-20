param(
    [ValidateRange(1, 100)]
    [int]$SampleLimit = 20
)

function Wait-ForHealthyContainer {
    param(
        [Parameter(Mandatory)]
        [string]$ContainerName,

        [int]$TimeoutSeconds = 90
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $healthStatus = & podman inspect `
            --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' `
            $ContainerName 2>$null

        if ($LASTEXITCODE -eq 0 -and $healthStatus.Trim() -in @("healthy", "running")) {
            return
        }

        Start-Sleep -Seconds 1
    }

    Write-Error "Timed out waiting for $ContainerName to become healthy."
    exit 1
}

# The audit needs Kafka and MinIO but not the collector or streaming raw sink.
# Starting existing service containers preserves their named data volumes.
Write-Host "Ensuring Kafka and MinIO are running..."
& podman compose up -d kafka minio
if ($LASTEXITCODE -ne 0) {
    Write-Error "Could not start the Kafka and MinIO audit dependencies."
    exit $LASTEXITCODE
}

Wait-ForHealthyContainer -ContainerName "crypto-trading-bot_kafka_1"
Wait-ForHealthyContainer -ContainerName "crypto-trading-bot_minio_1"

$auditArguments = @(
    "compose",
    "run",
    "--rm",
    "--no-deps",
    "-T",
    "-e",
    "RAW_AUDIT_SAMPLE_LIMIT=$SampleLimit",
    "raw-sink",
    "/opt/spark/bin/spark-submit",
    "--master",
    "local[2]",
    "--conf",
    "spark.jars.ivy=/opt/spark/.ivy2",
    "--packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.hadoop:hadoop-aws:3.3.4",
    "/opt/spark/work-dir/jobs/spark/entrypoints/audit_raw_market_trades.py"
)

& podman @auditArguments
exit $LASTEXITCODE
