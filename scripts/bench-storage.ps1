param(
    [string]$Bench = "BenchmarkVectorStore",
    [int]$Seconds = 10
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")

Push-Location $repoRoot
try {
    go test ./internal/storage -run '^$' -bench $Bench -benchtime "${Seconds}s" -benchmem
}
finally {
    Pop-Location
}

