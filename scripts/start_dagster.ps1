$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Test-Path (Join-Path $projectRoot ".env"))) {
    throw "Missing .env. Copy .env.example to .env and configure it first."
}

$runtimeDirectory = Join-Path $projectRoot ".dagster"
$trackedConfig = Join-Path $projectRoot "config\dagster.yaml"
$runtimeConfig = Join-Path $runtimeDirectory "dagster.yaml"

New-Item -ItemType Directory -Force $runtimeDirectory | Out-Null
Copy-Item -Path $trackedConfig -Destination $runtimeConfig -Force
$env:DAGSTER_HOME = (Resolve-Path $runtimeDirectory).Path

Write-Host "DAGSTER_HOME=$env:DAGSTER_HOME"
Write-Host "Using instance config: $runtimeConfig"
dagster dev -m kedra_scraper.definitions
