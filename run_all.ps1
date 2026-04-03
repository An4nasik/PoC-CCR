Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not $env:CCR_INDEX) {
    $env:CCR_INDEX = "rag_data"
}

function Wait-Cluster {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSeconds = 120
    )

    $started = Get-Date
    while (((Get-Date) - $started).TotalSeconds -lt $TimeoutSeconds) {
        try {
            $null = Invoke-RestMethod -Uri "$Url/_cluster/health" -TimeoutSec 3
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "Cluster is not ready: $Url"
}

function Stop-ProcessSafe {
    param(
        [Parameter(Mandatory = $true)][int]$Id
    )
    if (Get-Process -Id $Id -ErrorAction SilentlyContinue) {
        Stop-Process -Id $Id
    }
}

Write-Host "1) Install python dependency"
try {
    python -m pip install requests --disable-pip-version-check -q
} catch {
    Write-Host "Skip pip install. Using existing python environment."
}

Write-Host "2) Reset and start docker stack"
docker compose down -v | Out-Null
docker compose up -d | Out-Null

Write-Host "3) Wait clusters"
Wait-Cluster -Url "http://localhost:9200"
Wait-Cluster -Url "http://localhost:9201"

Write-Host "4) Setup CCR index=$($env:CCR_INDEX)"
python setup.py

Write-Host "5) Start producer on leader for 8 seconds"
$env:OPENSEARCH_URL = "http://localhost:9200"
$leaderProducer = Start-Process python -ArgumentList "producer.py" -PassThru
Start-Sleep -Seconds 8
Stop-ProcessSafe -Id $leaderProducer.Id
Start-Sleep -Seconds 2

$beforeLeader = (Invoke-RestMethod -Uri "http://localhost:9200/$($env:CCR_INDEX)/_count" -TimeoutSec 8).count
$beforeFollower = (Invoke-RestMethod -Uri "http://localhost:9201/$($env:CCR_INDEX)/_count" -TimeoutSec 8).count
Write-Host "Counts before failover: leader=$beforeLeader follower=$beforeFollower"

Write-Host "6) Simulate leader outage"
docker stop os-cluster-1 | Out-Null
Start-Sleep -Seconds 3

Write-Host "7) Run failover"
python failover.py

Write-Host "8) Start producer on new leader for 8 seconds"
$env:OPENSEARCH_URL = "http://localhost:9201"
$followerProducer = Start-Process python -ArgumentList "producer.py" -PassThru
Start-Sleep -Seconds 8
Stop-ProcessSafe -Id $followerProducer.Id
Start-Sleep -Seconds 2

$afterFollower = (Invoke-RestMethod -Uri "http://localhost:9201/$($env:CCR_INDEX)/_count" -TimeoutSec 8).count
$statusFollower = (Invoke-RestMethod -Uri "http://localhost:9201/_plugins/_replication/$($env:CCR_INDEX)/_status" -TimeoutSec 8).status

$leaderState = "up"
try {
    $null = Invoke-RestMethod -Uri "http://localhost:9200/_cluster/health" -TimeoutSec 3
} catch {
    $leaderState = "down"
}

Write-Host "9) Final check"
Write-Host "cluster-1=$leaderState"
Write-Host "cluster-2 replication status=$statusFollower"
Write-Host "cluster-2 documents=$afterFollower"
python check_status.py

Write-Host "Done"
