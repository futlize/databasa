param(
    [string]$Address = "10.0.0.100:50051",
    [string]$Collection = "benchmark_insert_200k",
    [int]$Dimension = 128,
    [int]$BatchSize = 1000,
    [int]$Workers = 4,
    [switch]$Embedded
)

$ErrorActionPreference = "Stop"
$embeddedValue = if ($Embedded.IsPresent) { "true" } else { "false" }
Write-Host "Insert count is fixed to 200000 for safety and fast validation."
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")

Push-Location $repoRoot
try {
    go run ./cmd/benchmark `
        -collection $Collection `
        -dimension $Dimension `
        -batch-size $BatchSize `
        -workers $Workers `
        "-embedded=$embeddedValue" `
        -addr $Address
}
finally {
    Pop-Location
}
