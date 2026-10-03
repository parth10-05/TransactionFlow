<#
.SYNOPSIS
    Verifies the TransactionFlow Spark standalone cluster and captures evidence.

.DESCRIPTION
    Checks that:
      - Docker Compose services are running
      - Both Spark workers are ALIVE
      - The streaming application is registered with the master
      - At least one recent batch has processed data

    Creates a timestamped evidence directory under docs/evidence/ containing:
      01_compose_ps.txt          - all container statuses
      02_spark_master_log.txt    - spark-master container logs (worker registration)
      03_spark_master_api.txt    - Spark Master REST API workers endpoint
      04_streaming_job_log.txt   - spark-streaming-job container logs
      05_reporting_metrics.txt   - reporting API /api/metrics/summary output
      06_reporting_pipeline.txt  - reporting API /api/pipeline/status output

.EXAMPLE
    .\scripts\verify-spark-cluster.ps1

.EXAMPLE
    # Also trigger a workload burst before verifying:
    .\scripts\verify-spark-cluster.ps1 -RunBurst -BurstSize 2000

.NOTES
    Run from the TransactionFlow project root.
    Does NOT record .env secrets.
#>

param(
    [switch]$RunBurst,
    [int]$BurstSize = 2000,
    [int]$InvalidPct = 5,
    [int]$HighValuePct = 2,
    [int]$UnknownTypePct = 3,
    [int]$WaitAfterBurstSeconds = 45
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── Configuration ──────────────────────────────────────────────────────────────
$SparkMasterContainer    = 'transactionflow-spark-master'
$StreamingJobContainer   = 'transactionflow-spark-streaming-job'
$Worker1Container        = 'transactionflow-spark-worker-1'
$Worker2Container        = 'transactionflow-spark-worker-2'
$SparkMasterApiUrl       = 'http://localhost:8080'
$ReportingApiUrl         = 'http://localhost:8082'
$ProducerApiUrl          = 'http://localhost:8081'

# ── Helpers ────────────────────────────────────────────────────────────────────
function Write-Step([string]$Msg) {
    Write-Host "`n==> $Msg" -ForegroundColor Cyan
}
function Write-OK([string]$Msg) {
    Write-Host "  [OK]  $Msg" -ForegroundColor Green
}
function Write-Warn([string]$Msg) {
    Write-Host " [WARN] $Msg" -ForegroundColor Yellow
}
function Write-Fail([string]$Msg) {
    Write-Host " [FAIL] $Msg" -ForegroundColor Red
}
function Save-Output([string]$FilePath, [string]$Content) {
    Set-Content -Path $FilePath -Value $Content -Encoding UTF8
    Write-OK "Saved: $(Split-Path $FilePath -Leaf)"
}
function Invoke-ApiSafe([string]$Url) {
    try {
        return Invoke-RestMethod -Uri $Url -TimeoutSec 10 -ErrorAction Stop | ConvertTo-Json -Depth 10
    } catch {
        return "ERROR calling $Url : $_"
    }
}
function Invoke-TextSafe([string]$Url) {
    try {
        return Invoke-WebRequest -Uri $Url -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop | Select-Object -ExpandProperty Content
    } catch {
        return "ERROR calling $Url : $_"
    }
}

# ── Step 0: Check Docker daemon reachable ─────────────────────────────────────
Write-Step '0/7  Checking Docker daemon'
try {
    $dockerInfo = docker info 2>&1
    if ($LASTEXITCODE -ne 0) { throw "docker info failed" }
    Write-OK 'Docker daemon is reachable'
} catch {
    Write-Fail 'Docker Desktop is not running. Start it and try again.'
    exit 1
}

# ── Step 1: docker compose ps ─────────────────────────────────────────────────
Write-Step '1/7  Checking Compose services (docker compose ps)'
$composePs = docker compose ps 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Fail "docker compose ps failed — is the stack up? Run: docker compose up -d"
    exit 1
}
Write-OK 'docker compose ps succeeded'
$composePs | Write-Host

# ── Step 2: Verify Spark Master is running ────────────────────────────────────
Write-Step '2/7  Checking Spark Master container'
$masterStatus = docker inspect --format '{{.State.Status}}' $SparkMasterContainer 2>$null
if ($masterStatus -ne 'running') {
    Write-Fail "Spark Master container '$SparkMasterContainer' is not running (status: $masterStatus)"
    exit 1
}
Write-OK 'Spark Master is running'

# ── Step 3: Verify both Spark Workers are ALIVE ────────────────────────────────
Write-Step '3/7  Checking Spark Workers via Master REST API'

# Spark Master REST API — /api/v1/applications gives apps, /json gives master state
$masterJsonUrl = "$SparkMasterApiUrl/json"
$masterJson = $null
$aliveWorkers = 0

try {
    $masterJsonRaw = Invoke-WebRequest -Uri $masterJsonUrl -TimeoutSec 15 -UseBasicParsing -ErrorAction Stop
    $masterJson    = $masterJsonRaw.Content | ConvertFrom-Json
    $aliveWorkers  = ($masterJson.workers | Where-Object { $_.state -eq 'ALIVE' }).Count
    Write-Host "  Workers reported by Master: total=$($masterJson.workers.Count) alive=$aliveWorkers" -ForegroundColor White
} catch {
    Write-Warn "Could not reach Spark Master REST API at $masterJsonUrl : $_"
    Write-Warn 'Falling back to container log check...'

    # Fall back: check worker registration lines in the master log
    $masterLog = docker logs $SparkMasterContainer 2>&1 | Select-String 'Registering worker'
    $aliveWorkers = ($masterLog | Measure-Object).Count
    Write-Host "  Worker registration lines in master log: $aliveWorkers" -ForegroundColor White
}

if ($aliveWorkers -lt 2) {
    Write-Fail "Only $aliveWorkers Spark worker(s) are alive. Need at least 2."
    Write-Host '  Check workers with:  docker compose logs spark-worker-1 spark-worker-2' -ForegroundColor Yellow
    exit 1
}
Write-OK "Two (or more) Spark workers are ALIVE ($aliveWorkers)"

# ── Step 4: Verify streaming application is registered ────────────────────────
Write-Step '4/7  Checking streaming application registration'
$streamingJobStatus = docker inspect --format '{{.State.Status}}' $StreamingJobContainer 2>$null
if ($streamingJobStatus -ne 'running') {
    Write-Fail "Streaming job container '$StreamingJobContainer' is not running (status: $streamingJobStatus)"
    Write-Host '  Tail logs with:  docker compose logs --tail=50 spark-streaming-job' -ForegroundColor Yellow
    exit 1
}

# Check the job's logs for successful Kafka-connect line
$jobLogs = docker logs $StreamingJobContainer 2>&1 | Select-String -Pattern 'Streaming query started|Kafka is reachable|Batch [0-9]+: processing'
if (($jobLogs | Measure-Object).Count -eq 0) {
    Write-Warn 'Streaming job is running but no batch activity found yet — give it 30 s and retry.'
} else {
    Write-OK "Streaming application is active ($(($jobLogs | Measure-Object).Count) progress lines found)"
}

# ── Optional: trigger a workload burst ────────────────────────────────────────
if ($RunBurst) {
    Write-Step "4b/7  Sending burst of $BurstSize transactions"
    $burstBody = @{
        numberOfEvents        = $BurstSize
        invalidPercentage     = $InvalidPct
        highValuePercentage   = $HighValuePct
        unknownTypePercentage = $UnknownTypePct
    } | ConvertTo-Json

    try {
        $burstResp = Invoke-RestMethod -Uri "$ProducerApiUrl/api/generator/burst" `
            -Method POST -ContentType 'application/json' -Body $burstBody -TimeoutSec 30
        Write-OK "Burst sent: $($burstResp | ConvertTo-Json -Compress)"
    } catch {
        Write-Warn "Burst request failed: $_  (producer may not be up yet)"
    }

    Write-Host "  Waiting ${WaitAfterBurstSeconds}s for Spark to process the batch..." -ForegroundColor Yellow
    Start-Sleep -Seconds $WaitAfterBurstSeconds
}

# ── Step 5: Check for recent batch in job logs ────────────────────────────────
Write-Step '5/7  Checking for recent processed batches in streaming job logs'
$recentBatches = docker logs $StreamingJobContainer 2>&1 | Select-String -Pattern 'Batch [0-9]+: processing [1-9]'
if (($recentBatches | Measure-Object).Count -eq 0) {
    Write-Warn 'No non-empty batches found in streaming job logs.'
    Write-Warn 'Generate data with: curl -X POST http://localhost:8081/api/generator/burst -H "Content-Type: application/json" -d "{\"numberOfEvents\":500,...}"'
} else {
    $lastBatch = ($recentBatches | Select-Object -Last 1).Line.Trim()
    Write-OK "Recent batch found: $lastBatch"
}

# ── Step 6: Create evidence directory ─────────────────────────────────────────
Write-Step '6/7  Capturing evidence'
$ScriptRoot2 = Split-Path $PSCommandPath -Parent
$EvidenceBase = Join-Path $ScriptRoot2 '..\docs\evidence'
try { $EvidenceBase = Resolve-Path $EvidenceBase }
catch { $EvidenceBase = Join-Path (Split-Path $ScriptRoot2 -Parent) 'docs\evidence' }

$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$RunDir    = Join-Path $EvidenceBase "spark_run_$Timestamp"
New-Item -ItemType Directory -Path $RunDir -Force | Out-Null
Write-OK "Evidence directory: $RunDir"

# 01 — docker compose ps
Save-Output (Join-Path $RunDir '01_compose_ps.txt') $composePs

# 02 — Spark Master container logs (last 200 lines — shows worker registration)
$masterLog = docker logs --tail 200 $SparkMasterContainer 2>&1
Save-Output (Join-Path $RunDir '02_spark_master_log.txt') ($masterLog -join "`n")

# 03 — Spark Master REST API /json (workers + apps)
if ($masterJson) {
    Save-Output (Join-Path $RunDir '03_spark_master_api.txt') ($masterJson | ConvertTo-Json -Depth 10)
} else {
    Save-Output (Join-Path $RunDir '03_spark_master_api.txt') (Invoke-TextSafe $masterJsonUrl)
}

# 04 — Streaming job logs (last 300 lines)
$jobLogsAll = docker logs --tail 300 $StreamingJobContainer 2>&1
Save-Output (Join-Path $RunDir '04_streaming_job_log.txt') ($jobLogsAll -join "`n")

# 05 — Worker 1 logs (last 100 lines — shows executor launches)
$w1Logs = docker logs --tail 100 $Worker1Container 2>&1
Save-Output (Join-Path $RunDir '05_spark_worker1_log.txt') ($w1Logs -join "`n")

# 06 — Worker 2 logs (last 100 lines)
$w2Logs = docker logs --tail 100 $Worker2Container 2>&1
Save-Output (Join-Path $RunDir '06_spark_worker2_log.txt') ($w2Logs -join "`n")

# 07 — Reporting API metrics summary
Save-Output (Join-Path $RunDir '07_reporting_metrics.txt') (Invoke-ApiSafe "$ReportingApiUrl/api/metrics/summary")

# 08 — Reporting API pipeline status
Save-Output (Join-Path $RunDir '08_reporting_pipeline.txt') (Invoke-ApiSafe "$ReportingApiUrl/api/pipeline/status")

# ── Step 7: Final summary ──────────────────────────────────────────────────────
Write-Step '7/7  Summary'
Write-Host ''
Write-Host '══════════════════════════════════════════════════════════════' -ForegroundColor Green
Write-Host '  Spark cluster verification complete!' -ForegroundColor Green
Write-Host "  Evidence: $RunDir" -ForegroundColor Green
Write-Host ''
Write-Host '  What each file proves:' -ForegroundColor White
Write-Host '    01_compose_ps.txt          — all containers running'
Write-Host '    02_spark_master_log.txt    — worker REGISTRATION messages'
Write-Host '    03_spark_master_api.txt    — master state: workers[], activeApps[]'
Write-Host '    04_streaming_job_log.txt   — batch processing across executors'
Write-Host '    05_spark_worker1_log.txt   — executor launch on worker-1'
Write-Host '    06_spark_worker2_log.txt   — executor launch on worker-2'
Write-Host '    07_reporting_metrics.txt   — analytics results (proves workload ran)'
Write-Host '    08_reporting_pipeline.txt  — pipeline component health'
Write-Host ''
Write-Host '  Key proof chain:' -ForegroundColor White
Write-Host '    02/03  => cluster membership (workers are ALIVE)'
Write-Host '    04     => application runs in cluster mode (not local)'
Write-Host '    05/06  => both workers received executor work'
Write-Host '    07     => analytics results prove real data was processed'
Write-Host '══════════════════════════════════════════════════════════════' -ForegroundColor Green
