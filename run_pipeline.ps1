# =====================================================================
# run_pipeline.ps1 - IoT pipeline launcher
# - auto-creates venv + installs requirements.txt on first run
# - auto-creates Kafka container if missing
# - auto-starts Docker Desktop if not running
# - starts Flask API + Aiven consumer + HTTP generator
# =====================================================================
$ErrorActionPreference = "Stop"

$AppDir = $PSScriptRoot
$VenvDir = Join-Path $AppDir ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$Requirements = Join-Path $AppDir "requirements.txt"
$ApiFile = Join-Path $AppDir "kafka_api_flask.py"
$ConsumerFile = Join-Path $AppDir "kafka_consumer_aiven.py"
$GeneratorFile = Join-Path $AppDir "sensor_generator_http.py"
$LogDir = Join-Path $AppDir "logs"

$KafkaContainer = "kafka-iot"
$KafkaImage = "apache/kafka:3.7.0"
$ApiUrl = "http://127.0.0.1:8001"

function Cyan ($m) { Write-Host $m -ForegroundColor Cyan }
function Green($m) { Write-Host $m -ForegroundColor Green }
function Yellow($m) { Write-Host $m -ForegroundColor Yellow }
function Red  ($m) { Write-Host $m -ForegroundColor Red }
function Step ($n, $m) { Write-Host ""; Cyan "==== $n  $m ====" }

# ------------------------------------------------------------------ #
Step "1/9" "Check Python"
# ------------------------------------------------------------------ #
$pythonCmd = $null
foreach ($cmd in @("python", "py -3", "python3")) {
    try {
        $v = & $cmd --version 2>$null
        if ($LASTEXITCODE -eq 0 -and $v -match "Python 3\.(\d+)") {
            $minor = [int]$matches[1]
            if ($minor -ge 10) { $pythonCmd = $cmd; break }
        }
    }
    catch { }
}
if (-not $pythonCmd) {
    Red "Python 3.10+ not found. Install from https://www.python.org/downloads/ and rerun."
    exit 1
}
Green "python found: $(Invoke-Expression "$pythonCmd --version")"

# ------------------------------------------------------------------ #
Step "2/9" "Ensure venv (auto-creates on first run)"
# ------------------------------------------------------------------ #
if (-not (Test-Path $VenvPython)) {
    Yellow "venv missing - creating..."
    Invoke-Expression "$pythonCmd -m venv `"$VenvDir`""
    if (-not (Test-Path $VenvPython)) { Red "venv creation failed"; exit 1 }
    Green "venv created at $VenvDir"
}
else { Green "venv exists" }

# ------------------------------------------------------------------ #
Step "3/9" "Install dependencies (auto on first run)"
# ------------------------------------------------------------------ #
$sentinel = Join-Path $VenvDir ".deps-installed"
if (-not (Test-Path $sentinel) -or ((Get-Item $sentinel).LastWriteTime -lt (Get-Item $Requirements).LastWriteTime)) {
    Yellow "installing requirements.txt (first run, ~1 minute)..."
    & $VenvPython -m pip install --upgrade pip --quiet
    & $VenvPython -m pip install -r $Requirements --quiet
    if ($LASTEXITCODE -ne 0) { Red "pip install failed"; exit 1 }
    New-Item -ItemType File -Path $sentinel -Force | Out-Null
    Green "dependencies installed"
}
else { Green "dependencies already installed" }

# ------------------------------------------------------------------ #
Step "4/9" "Ensure Docker Desktop is running"
# ------------------------------------------------------------------ #
$null = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    $dockerExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerExe) {
        Yellow "starting Docker Desktop..."
        Start-Process $dockerExe
        for ($i = 0; $i -lt 60; $i++) {
            Start-Sleep 2
            $null = docker info 2>&1
            if ($LASTEXITCODE -eq 0) { break }
            Write-Host -NoNewline "."
        }
        Write-Host ""
    }
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) { Red "Docker not available - open Docker Desktop manually and rerun"; exit 1 }
}
Green "docker OK"

# ------------------------------------------------------------------ #
Step "5/9" "Ensure Kafka container (auto-creates on first run)"
# ------------------------------------------------------------------ #
$running = docker ps --filter "name=^$KafkaContainer$" --format "{{.Names}}"
if ($running -ne $KafkaContainer) {
    $exists = docker ps -a --filter "name=^$KafkaContainer$" --format "{{.Names}}"
    if ($exists -eq $KafkaContainer) {
        Yellow "starting existing $KafkaContainer"
        docker start $KafkaContainer | Out-Null
    }
    else {
        Yellow "creating $KafkaContainer (first run, ~30s)..."
        docker run -d --name $KafkaContainer -p 9092:9092 `
            -e KAFKA_NODE_ID=1 `
            -e KAFKA_PROCESS_ROLES=broker, controller `
            -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" `
            -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:9092" `
            -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
            -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" `
            -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093" `
            -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
            -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
            -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
            $KafkaImage | Out-Null
        Green "container created"
    }
}
else { Green "$KafkaContainer already running" }

# wait for broker
$ok = $false
for ($i = 1; $i -le 60; $i++) {
    docker exec $KafkaContainer /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $ok = $true; break }
    Write-Host -NoNewline "."; Start-Sleep 1
}
if ($ok) { Green "Kafka READY on 9092" } else { Red "Kafka not ready"; exit 1 }

# create topic (idempotent)
docker exec $KafkaContainer /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sensor-readings --partitions 3 --replication-factor 1 | Out-Null
Green "topic sensor-readings ready"

# ------------------------------------------------------------------ #
Step "6/9" "Check AIVEN_URL env var"
# ------------------------------------------------------------------ #
if (-not $env:AIVEN_URL) {
    Yellow "AIVEN_URL is not set."
    Yellow "Set it in this session then rerun, e.g.:"
    Yellow '  $env:AIVEN_URL = "postgres://avnadmin:PASSWORD@HOST:PORT/defaultdb?sslmode=require"'
    Yellow "Or add it to your user environment permanently:"
    Yellow '  [Environment]::SetEnvironmentVariable("AIVEN_URL","postgres://...","User")'
    exit 1
}
Green "AIVEN_URL is set"

# ------------------------------------------------------------------ #
Step "7/9" "Stop any previous pipeline processes"
# ------------------------------------------------------------------ #
for ($round = 1; $round -le 6; $round++) {
    $targets = @()
    foreach ($p in Get-Process python* -ErrorAction SilentlyContinue) {
        $cmd = (Get-CimInstance Win32_Process -Filter "ProcessId=$($p.Id)" -ErrorAction SilentlyContinue).CommandLine
        if ($cmd -and $cmd -match [regex]::Escape($AppDir)) { $targets += $p }
    }
    if ($targets.Count -eq 0) { break }
    Yellow "round $round - killing $($targets.Count) old process(es)"
    foreach ($p in $targets) {
        try { Stop-Process -Id $p.Id -Force -ErrorAction Stop }
        catch { & taskkill /F /PID $p.Id 2>&1 | Out-Null }
    }
    Start-Sleep 2
}
Green "clean slate"

# ------------------------------------------------------------------ #
Step "8/9" "Launch Flask API + Aiven consumer + generator"
# ------------------------------------------------------------------ #
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Start-Process -FilePath $VenvPython -ArgumentList "-u `"$ApiFile`"" -WorkingDirectory $AppDir `
    -RedirectStandardOutput (Join-Path $LogDir "api.log") `
    -RedirectStandardError  (Join-Path $LogDir "api.err.log") -WindowStyle Hidden
$healthy = $false
for ($i = 1; $i -le 20; $i++) {
    try {
        $r = Invoke-RestMethod -Uri "$ApiUrl/health" -TimeoutSec 8 -ErrorAction Stop
        $healthy = $true; Green "API up: $($r | ConvertTo-Json -Compress)"; break
    }
    catch { Write-Host -NoNewline "."; Start-Sleep 2 }
}
if (-not $healthy) { Red "API did not respond"; Get-Content (Join-Path $LogDir "api.err.log") -Tail 30; exit 1 }

Start-Process -FilePath $VenvPython -ArgumentList "-u `"$ConsumerFile`"" -WorkingDirectory $AppDir `
    -RedirectStandardOutput (Join-Path $LogDir "consumer.log") `
    -RedirectStandardError  (Join-Path $LogDir "consumer.err.log") -WindowStyle Hidden
Start-Sleep 6
Write-Host "  consumer.log tail:"
Get-Content (Join-Path $LogDir "consumer.log") -Tail 8 -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "    $_" }

Start-Process -FilePath $VenvPython -ArgumentList "-u `"$GeneratorFile`"" -WorkingDirectory $AppDir `
    -RedirectStandardOutput (Join-Path $LogDir "generator.log") `
    -RedirectStandardError  (Join-Path $LogDir "generator.err.log") -WindowStyle Hidden
Start-Sleep 5
Write-Host "  generator.log tail:"
Get-Content (Join-Path $LogDir "generator.log") -Tail 5 -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "    $_" }

# ------------------------------------------------------------------ #
# -------lunch anomaly_detector.py and telegram_alerter.py---------- #
# ------------------------------------------------------------------ #
Start-Process -FilePath $VenvPython -ArgumentList "-u `"$AppDir\anomaly_detector.py`"" -WorkingDirectory $AppDir `
    -RedirectStandardOutput (Join-Path $LogDir "detector.log") `
    -RedirectStandardError  (Join-Path $LogDir "detector.err.log") -WindowStyle Hidden

Start-Process -FilePath $VenvPython -ArgumentList "-u `"$AppDir\telegram_alerter.py`"" -WorkingDirectory $AppDir `
    -RedirectStandardOutput (Join-Path $LogDir "telegram.log") `
    -RedirectStandardError  (Join-Path $LogDir "telegram.err.log") -WindowStyle Hidden
# ------------------------------------------------------------------ #

# ------------------------------------------------------------------ #
Step "9/9" "Verify"
# ------------------------------------------------------------------ #
Start-Sleep 3
$scripts = @()
foreach ($p in Get-Process python* -ErrorAction SilentlyContinue) {
    $o = Get-CimInstance Win32_Process -Filter "ProcessId=$($p.Id)" -ErrorAction SilentlyContinue
    if ($o -and $o.CommandLine -match '([\w\-]+\.py)') { $scripts += $matches[1] }
}
$uniqueScripts = @($scripts | Sort-Object -Unique)
if ($uniqueScripts.Count -eq 3) {
    Green "3 active scripts: $($uniqueScripts -join ', ')"
}
else {
    Yellow "found $($uniqueScripts.Count) scripts: $($uniqueScripts -join ', ')"
}

Write-Host ""
Cyan "==== Pipeline is LIVE ===="
Write-Host "  API          : $ApiUrl/health"
Write-Host "  Logs         : $LogDir"
Write-Host "  Aiven table  : readings (in defaultdb)"
Write-Host ""
Write-Host "  Watch live   : Get-Content logs\consumer.log -Wait -Tail 20"
Write-Host "  Stop         : .\stop_pipeline.ps1"
Write-Host ""
