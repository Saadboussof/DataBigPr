param(
    [string]$DataDir = "./data",
    [string]$MinioContainer = "taasim-minio",
    [string]$AccessKey = "admin",
    [string]$SecretKey = "password",
    [string]$RawBucket = "raw",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' not found in PATH."
    }
}

function Get-MinioNetwork {
    param([string]$ContainerName)

    $network = docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' $ContainerName 2>$null |
        Select-Object -First 1

    if (-not $network) {
        throw "Could not resolve Docker network for container '$ContainerName'."
    }

    return $network.Trim()
}

Require-Command -Name "docker"

$resolvedDataDir = Resolve-Path -Path $DataDir -ErrorAction Stop
$trainCsv = Join-Path $resolvedDataDir "train.csv"
$zoneMapping = Join-Path $resolvedDataDir "zone_mapping.csv"

if (-not (Test-Path $trainCsv)) {
    throw "Missing required file: $trainCsv"
}

$nycParquetFiles = Get-ChildItem -Path $resolvedDataDir -File -Filter "yellow_tripdata_*.parquet" |
    Sort-Object -Property Name

if ($nycParquetFiles.Count -eq 0) {
    throw "No files matched pattern yellow_tripdata_*.parquet in $resolvedDataDir"
}

$minioRunning = (docker inspect -f '{{.State.Running}}' $MinioContainer 2>$null).Trim()
if ($minioRunning -ne "true") {
    Write-Host "MinIO container '$MinioContainer' is not running. Starting it with docker compose..."
    docker compose up -d minio minio-init | Out-Host

    $minioRunning = (docker inspect -f '{{.State.Running}}' $MinioContainer 2>$null).Trim()
    if ($minioRunning -ne "true") {
        throw "Failed to start MinIO container '$MinioContainer'."
    }
}

$network = Get-MinioNetwork -ContainerName $MinioContainer

$mcCommands = New-Object System.Collections.Generic.List[string]
$mcCommands.Add("mc alias set local http://${MinioContainer}:9000 $AccessKey $SecretKey")
$mcCommands.Add("mc mb --ignore-existing local/$RawBucket")
$mcCommands.Add("mc cp /upload/train.csv local/$RawBucket/porto/train.csv")

foreach ($file in $nycParquetFiles) {
    $name = $file.Name
    $mcCommands.Add("mc cp /upload/$name local/$RawBucket/nyc/$name")
}

if (Test-Path $zoneMapping) {
    $mcCommands.Add("mc cp /upload/zone_mapping.csv local/$RawBucket/reference/zone_mapping.csv")
}

$mcCommands.Add("mc ls --recursive local/$RawBucket")
$mcCommandLine = ($mcCommands -join "; ")

Write-Host "Upload plan:"
Write-Host "- train.csv -> s3a://$RawBucket/porto/train.csv"
Write-Host "- yellow_tripdata_*.parquet -> s3a://$RawBucket/nyc/"
if (Test-Path $zoneMapping) {
    Write-Host "- zone_mapping.csv -> s3a://$RawBucket/reference/zone_mapping.csv"
}

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run enabled. No upload performed."
    Write-Host "docker run --rm --network $network --entrypoint /bin/sh -v ${resolvedDataDir}:/upload:ro minio/mc -c \"$mcCommandLine\""
    exit 0
}

Write-Host ""
Write-Host "Uploading local raw data to MinIO..."

docker run --rm --network $network --entrypoint /bin/sh -v "${resolvedDataDir}:/upload:ro" minio/mc -c "$mcCommandLine" | Out-Host

Write-Host ""
Write-Host "Upload finished successfully."
