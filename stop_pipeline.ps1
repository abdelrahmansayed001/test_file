$AppDir = $PSScriptRoot
Write-Host "Stopping Python processes from $AppDir ..." -ForegroundColor Cyan
$killed = 0
foreach ($p in Get-Process python* -ErrorAction SilentlyContinue) {
    $cmd = (Get-CimInstance Win32_Process -Filter "ProcessId=$($p.Id)" -ErrorAction SilentlyContinue).CommandLine
    if ($cmd -and $cmd -match [regex]::Escape($AppDir)) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        Write-Host "  stopped pid $($p.Id)"
        $killed++
    }
}
if ($killed -eq 0) { Write-Host "  nothing to stop" -ForegroundColor Yellow }
docker ps --format "  {{.Names}}`t{{.Status}}"
