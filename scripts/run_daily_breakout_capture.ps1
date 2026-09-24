param(
    [ValidateSet('register', 'collect', 'evaluate')]
    [string]$Action = 'collect'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExecutable = Join-Path $projectRoot '.venv311/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $pythonExecutable)) {
    throw 'Create the project Python 3.11 environment at .venv311 before running this command.'
}

# This digest is the registration input, never recalculated from an edited spec.
$specDigest = '41551c83ec1e42a5dcb4be61ad6ffc889afb171a94300245c27daa55d4e18084'
$logDirectory = Join-Path $projectRoot 'artifacts/strategy_experiments/daily_breakout_v1/logs'
$null = New-Item -ItemType Directory -Path $logDirectory -Force
$logName = '{0}-{1}-{2}.txt' -f [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ'), $Action, [Guid]::NewGuid().ToString('N')
$previousPythonPath = $env:PYTHONPATH
$transcriptStarted = $false
$resultCode = 4
Push-Location -LiteralPath $projectRoot
try {
    $null = Start-Transcript -LiteralPath (Join-Path $logDirectory $logName)
    $transcriptStarted = $true
    $env:PYTHONPATH = @(
        (Join-Path $projectRoot 'apps/trading_core/src'),
        (Join-Path $projectRoot 'packages/exchange_adapters/src'),
        (Join-Path $projectRoot 'packages/domain/src')
    ) -join [IO.Path]::PathSeparator
    & $pythonExecutable -m crypto_trading_core.daily_breakout $Action `
        --spec experiments/btc-usd-daily-breakout-v1.json `
        --spec-sha256 $specDigest `
        --output artifacts/strategy_experiments/daily_breakout_v1
    $resultCode = $LASTEXITCODE
}
finally {
    $env:PYTHONPATH = $previousPythonPath
    if ($transcriptStarted) { $null = Stop-Transcript }
    Pop-Location
}
exit $resultCode
