<#
.SYNOPSIS
    Captures proof that HDFS data is genuinely distributed across two DataNodes.

.DESCRIPTION
    Creates a timestamped evidence directory under docs/evidence/ and writes:
      - docker compose ps                 (all containers running)
      - hdfs dfsadmin -report             (DataNode count + health)
      - hdfs dfs -ls -R /data/transactionflow   (canonical path listing)
      - hdfs fsck ... -files -blocks -locations  (block replica locations)

    Run this from the TransactionFlow project root after:
      1. docker compose up -d
      2. Generating at least one transaction burst (see README)
      3. Waiting for the Spark streaming job to produce at least one batch

.EXAMPLE
    .\scripts\capture-hdfs-distribution.ps1

.NOTES
    Safe to run repeatedly.  Each run creates a new timestamped subdirectory.
    Does NOT record .env credentials or any secrets.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── Configuration ──────────────────────────────────────────────────────────────
$NameNodeContainer = 'transactionflow-hdfs-namenode'
$RootHdfsPath      = '/data/transactionflow'
$RawHdfsPath       = '/data/transactionflow/raw/transactions'
$RejectedHdfsPath  = '/data/transactionflow/rejected/transactions'

# ── Helpers ────────────────────────────────────────────────────────────────────
function Write-Step([string]$Msg) {
    Write-Host "`n==> $Msg" -ForegroundColor Cyan
}

function Write-OK([string]$Msg) {
    Write-Host "  OK  $Msg" -ForegroundColor Green
}

function Write-Warn([string]$Msg) {
    Write-Host " WARN $Msg" -ForegroundColor Yellow
}

function Save-Output([string]$FilePath, [string]$Content) {
    Set-Content -Path $FilePath -Value $Content -Encoding UTF8
    Write-OK "Saved: $FilePath"
}

# ── Verify cluster is running ──────────────────────────────────────────────────
Write-Step 'Checking Docker Compose stack is up'
try {
    $psOutput = docker compose ps --format json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose ps failed: $psOutput"
    }
} catch {
    Write-Error "ERROR: Could not connect to Docker or the stack is not running.`nStart the stack first: docker compose up -d"
    exit 1
}

# Verify NameNode container exists and is running
$nnRunning = docker inspect --format '{{.State.Status}}' $NameNodeContainer 2>$null
if ($nnRunning -ne 'running') {
    Write-Error "ERROR: Container '$NameNodeContainer' is not running (status: $nnRunning).`nStart the stack first: docker compose up -d"
    exit 1
}
Write-OK "NameNode container is running"

# ── Verify HDFS has DataNode output ───────────────────────────────────────────
Write-Step 'Checking for HDFS output under canonical paths'
$rawLs = docker exec $NameNodeContainer hdfs dfs -ls "$RawHdfsPath" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Warn "No output yet under $RawHdfsPath — run the generator first:"
    Write-Host "  curl -X POST http://localhost:8081/api/generator/burst ``" -ForegroundColor White
    Write-Host "    -H 'Content-Type: application/json' ``" -ForegroundColor White
    Write-Host "    -d '{""numberOfEvents"":500,""invalidPercentage"":5,""highValuePercentage"":2,""unknownTypePercentage"":3}'" -ForegroundColor White
    Write-Host ''
    Write-Host 'Then wait ~30 seconds for the Spark batch to complete and re-run this script.' -ForegroundColor Yellow
    exit 1
}
Write-OK "HDFS output detected under $RawHdfsPath"

# ── Create evidence directory ──────────────────────────────────────────────────
$Timestamp  = Get-Date -Format 'yyyyMMdd_HHmmss'
$EvidenceDir = Join-Path $PSScriptRoot '..\docs\evidence' | Resolve-Path -ErrorAction SilentlyContinue
if (-not $EvidenceDir) {
    $EvidenceDir = Join-Path (Split-Path $PSScriptRoot) 'docs\evidence'
}
$RunDir = Join-Path $EvidenceDir "run_$Timestamp"
New-Item -ItemType Directory -Path $RunDir -Force | Out-Null
Write-OK "Evidence directory: $RunDir"

# ── 1. docker compose ps ───────────────────────────────────────────────────────
Write-Step '1/5  docker compose ps'
$composePs = docker compose ps 2>&1
Save-Output (Join-Path $RunDir '01_compose_ps.txt') $composePs

# ── 2. hdfs dfsadmin -report ───────────────────────────────────────────────────
Write-Step '2/5  hdfs dfsadmin -report'
$dfsReport = docker exec $NameNodeContainer hdfs dfsadmin -report 2>&1
Save-Output (Join-Path $RunDir '02_dfsadmin_report.txt') $dfsReport

# Check live DataNode count
$liveMatch = [regex]::Match($dfsReport, 'Live datanodes\s*\((\d+)\)')
if ($liveMatch.Success) {
    $liveCount = [int]$liveMatch.Groups[1].Value
    if ($liveCount -ge 2) {
        Write-OK "Live DataNodes: $liveCount (replication possible)"
    } else {
        Write-Warn "Only $liveCount live DataNode(s) — replication factor 2 requires at least 2"
    }
} else {
    Write-Warn 'Could not parse live DataNode count from dfsadmin report'
}

# ── 3. hdfs dfs -ls -R /data/transactionflow ──────────────────────────────────
Write-Step '3/5  hdfs dfs -ls -R (canonical path listing)'
$lsR = docker exec $NameNodeContainer hdfs dfs -ls -R $RootHdfsPath 2>&1
Save-Output (Join-Path $RunDir '03_ls_recursive.txt') $lsR

# ── 4. hdfs fsck — raw transactions ───────────────────────────────────────────
Write-Step '4/5  hdfs fsck — raw transactions (block locations)'
$fsckRaw = docker exec $NameNodeContainer hdfs fsck $RawHdfsPath -files -blocks -locations 2>&1
Save-Output (Join-Path $RunDir '04_fsck_raw.txt') $fsckRaw

# Extract replication summary
$replMatch = [regex]::Match($fsckRaw, 'Status:\s*(\w+)')
if ($replMatch.Success) {
    Write-OK "FSCK status (raw): $($replMatch.Groups[1].Value)"
}
$underMatch = [regex]::Match($fsckRaw, 'Under-replicated blocks:\s*(\d+)')
if ($underMatch.Success -and [int]$underMatch.Groups[1].Value -gt 0) {
    Write-Warn "Under-replicated blocks detected ($($underMatch.Groups[1].Value)) — DataNodes may not have fully registered yet"
}

# ── 5. hdfs fsck — rejected transactions (if files exist) ─────────────────────
Write-Step '5/5  hdfs fsck — rejected transactions (if files exist)'
$rejectedLs = docker exec $NameNodeContainer hdfs dfs -ls $RejectedHdfsPath 2>&1
if ($LASTEXITCODE -eq 0 -and $rejectedLs -match '\.json') {
    $fsckRej = docker exec $NameNodeContainer hdfs fsck $RejectedHdfsPath -files -blocks -locations 2>&1
    Save-Output (Join-Path $RunDir '05_fsck_rejected.txt') $fsckRej
    Write-OK "FSCK captured for rejected path"
} else {
    Save-Output (Join-Path $RunDir '05_fsck_rejected.txt') 'No rejected files present at capture time.'
    Write-Warn "No rejected files found at $RejectedHdfsPath (run with invalidPercentage > 0)"
}

# ── Final summary ──────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '══════════════════════════════════════════════════════════' -ForegroundColor Green
Write-Host '  Evidence capture complete!' -ForegroundColor Green
Write-Host "  Output: $RunDir" -ForegroundColor Green
Write-Host ''
Write-Host '  Key files:' -ForegroundColor White
Write-Host '    01_compose_ps.txt       — all containers running'
Write-Host '    02_dfsadmin_report.txt  — DataNode health + live count'
Write-Host '    03_ls_recursive.txt     — HDFS canonical path listing'
Write-Host '    04_fsck_raw.txt         — block replicas on DataNodes (PROOF)'
Write-Host '    05_fsck_rejected.txt    — block replicas for rejected files'
Write-Host ''
Write-Host '  What proves what:' -ForegroundColor White
Write-Host '    ls output     => file EXISTENCE'
Write-Host '    fsck output   => block DISTRIBUTION across DataNodes'
Write-Host '══════════════════════════════════════════════════════════' -ForegroundColor Green
