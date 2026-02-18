param(
    [string]$Address = "10.0.0.100:50051",
    [string]$Collection = "benchmark_insert_20m",
    [int]$Vectors = 20000000,
    [int]$Dimension = 128,
    [int]$BatchSize = 1000,
    [int]$Workers = 4,
    [switch]$Embedded
)

$ErrorActionPreference = "Stop"
$embeddedValue = if ($Embedded.IsPresent) { "true" } else { "false" }

go run ./cmd/databasa-benchmark `
    -vectors $Vectors `
    -collection $Collection `
    -dimension $Dimension `
    -batch-size $BatchSize `
    -workers $Workers `
    "-embedded=$embeddedValue" `
    -addr $Address
