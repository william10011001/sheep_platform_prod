$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$appDir = Join-Path $root "app"
Set-Location $appDir

$pythonCheck = @'
import platform
import sys

if sys.version_info[:2] != (3, 11):
    raise SystemExit(f"OpenNode packaging requires Python 3.11.x, current={platform.python_version()}")
if platform.architecture()[0] != "64bit":
    raise SystemExit("OpenNode packaging requires 64-bit Python.")
'@
$pythonCheck | python -

python -m pip install --upgrade pip pyinstaller
python -m PyInstaller --noconfirm --clean OpenNode.spec

$distDir = Join-Path $appDir "dist\\OpenNode-win-x64"
$zipPath = Join-Path $appDir "OpenNode-win-x64.zip"
if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}
Compress-Archive -Path "$distDir\\*" -DestinationPath $zipPath
Write-Host "OpenNode build complete:" -ForegroundColor Green
Write-Host "  Folder: $distDir"
Write-Host "  Zip:    $zipPath"
