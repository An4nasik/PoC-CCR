Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not $env:CCR_INDEX) {
    $env:CCR_INDEX = "rag_data"
}

$metaDocId = "__ccr_meta__"
$pythonExe = if (Test-Path -LiteralPath ".\.venv\Scripts\python.exe") {
    (Resolve-Path ".\.venv\Scripts\python.exe").Path
} else {
    "python"
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

function Assert-ProcessAlive {
    param(
        [Parameter(Mandatory = $true)][int]$Id,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if (-not (Get-Process -Id $Id -ErrorAction SilentlyContinue)) {
        throw "$Name exited unexpectedly"
    }
}

function Get-DataCount {
    param(
        [Parameter(Mandatory = $true)][string]$Url
    )

    $body = @{
        query = @{
            bool = @{
                must_not = @(
                    @{
                        ids = @{
                            values = @($metaDocId)
                        }
                    }
                )
            }
        }
    } | ConvertTo-Json -Depth 8

    return (
        Invoke-RestMethod `
            -Method Post `
            -Uri "$Url/$($env:CCR_INDEX)/_count" `
            -TimeoutSec 8 `
            -ContentType "application/json" `
            -Body $body
    ).count
}

function Invoke-Python {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    & $pythonExe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed: $($Arguments -join ' ')"
    }
}

$producer = $null
$consumer = $null

try {
    Write-Host "1) Reset and start docker stack"
    docker compose down -v | Out-Null
    docker compose up -d | Out-Null

    Write-Host "2) Wait clusters"
    Wait-Cluster -Url "http://localhost:9200"
    Wait-Cluster -Url "http://localhost:9201"

    Write-Host "3) Setup CCR index=$($env:CCR_INDEX)"
    Invoke-Python -Arguments @(".\setup.py")

    Write-Host "4) Start producer and consumer"
    $env:OPENSEARCH_URLS = "http://localhost:9200,http://localhost:9201"
    $producer = Start-Process -FilePath $pythonExe -ArgumentList "producer.py" -PassThru
    $consumer = Start-Process -FilePath $pythonExe -ArgumentList "consumer.py" -PassThru
    Start-Sleep -Seconds 8
    Assert-ProcessAlive -Id $producer.Id -Name "producer"
    Assert-ProcessAlive -Id $consumer.Id -Name "consumer"

    $beforeLeader = Get-DataCount -Url "http://localhost:9200"
    $beforeFollower = Get-DataCount -Url "http://localhost:9201"
    Write-Host "Counts before failover: cluster-1=$beforeLeader cluster-2=$beforeFollower"

    Write-Host "5) Simulate leader outage and failover to cluster-2"
    docker stop os-cluster-1 | Out-Null
    Start-Sleep -Seconds 3
    Invoke-Python -Arguments @(".\failover.py")
    Start-Sleep -Seconds 6
    Assert-ProcessAlive -Id $producer.Id -Name "producer"
    Assert-ProcessAlive -Id $consumer.Id -Name "consumer"

    $afterFirstFailover = Get-DataCount -Url "http://localhost:9201"
    Write-Host "Count after first failover: cluster-2=$afterFirstFailover"

    Write-Host "6) Restore cluster-1 and run failback"
    docker start os-cluster-1 | Out-Null
    Wait-Cluster -Url "http://localhost:9200"
    Invoke-Python -Arguments @(".\failback.py")
    Start-Sleep -Seconds 6
    Assert-ProcessAlive -Id $producer.Id -Name "producer"
    Assert-ProcessAlive -Id $consumer.Id -Name "consumer"

    $afterFailbackLeader = Get-DataCount -Url "http://localhost:9200"
    $afterFailbackFollower = Get-DataCount -Url "http://localhost:9201"
    Write-Host "Counts after failback: cluster-1=$afterFailbackLeader cluster-2=$afterFailbackFollower"

    Write-Host "7) Simulate leader outage on cluster-2 and failover back to cluster-1"
    docker stop os-cluster-2 | Out-Null
    Start-Sleep -Seconds 3
    Invoke-Python -Arguments @(".\failover.py")
    Start-Sleep -Seconds 6
    Assert-ProcessAlive -Id $producer.Id -Name "producer"
    Assert-ProcessAlive -Id $consumer.Id -Name "consumer"

    $finalCount = Get-DataCount -Url "http://localhost:9200"
    Write-Host "8) Final check"
    Write-Host "cluster-1 documents=$finalCount"
    Invoke-Python -Arguments @(".\check_status.py")
    Write-Host "Done"
} finally {
    if ($producer) {
        Stop-ProcessSafe -Id $producer.Id
    }
    if ($consumer) {
        Stop-ProcessSafe -Id $consumer.Id
    }
}
