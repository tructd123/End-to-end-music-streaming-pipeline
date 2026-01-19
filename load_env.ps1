# Load environment variables from .env file
# Usage: . .\load_env.ps1

$envFile = Join-Path $PSScriptRoot ".env"

if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
            Write-Host "Set $name" -ForegroundColor Green
        }
    }
    Write-Host "`nEnvironment loaded successfully!" -ForegroundColor Cyan
} else {
    Write-Host "Error: .env file not found at $envFile" -ForegroundColor Red
}
