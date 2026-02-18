param(
    [string]$Address = "10.0.0.100:50051",
    [string]$Collection = "benchmark",
    [int]$Dimension = 1536,
    [int]$BatchSize = 256,
    [int]$Workers = 4,
    [switch]$Embedded
)

$ErrorActionPreference = "Stop"
$embeddedValue = if ($Embedded.IsPresent) { "true" } else { "false" }
Write-Host "Insert count is fixed to 200000 for safety and fast validation."
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")

$prevGOOS = $env:GOOS
$prevGOARCH = $env:GOARCH
$prevGOARM = $env:GOARM
$hostGOOS = (go env GOHOSTOS).Trim()
$hostGOARCH = (go env GOHOSTARCH).Trim()

Push-Location $repoRoot
try {
    # Avoid inherited cross-compile env (e.g. GOOS=linux) breaking go run on Windows.
    $env:GOOS = $hostGOOS
    $env:GOARCH = $hostGOARCH
    Remove-Item Env:GOARM -ErrorAction SilentlyContinue

    go run ./cmd/benchmark `
        -collection $Collection `
        -dimension $Dimension `
        -batch-size $BatchSize `
        -workers $Workers `
        "-embedded=$embeddedValue" `
        -addr $Address
}
finally {
    if ($null -eq $prevGOOS -or $prevGOOS -eq "") {
        Remove-Item Env:GOOS -ErrorAction SilentlyContinue
    } else {
        $env:GOOS = $prevGOOS
    }

    if ($null -eq $prevGOARCH -or $prevGOARCH -eq "") {
        Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
    } else {
        $env:GOARCH = $prevGOARCH
    }

    if ($null -eq $prevGOARM -or $prevGOARM -eq "") {
        Remove-Item Env:GOARM -ErrorAction SilentlyContinue
    } else {
        $env:GOARM = $prevGOARM
    }

    Pop-Location
}
