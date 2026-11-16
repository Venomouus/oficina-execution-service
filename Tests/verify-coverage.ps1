param(
    [Parameter(Mandatory = $true)]
    [string] $ResultsDirectory
)

$ErrorActionPreference = 'Stop'
$reports = @(Get-ChildItem -LiteralPath $ResultsDirectory -Recurse -Filter 'coverage.cobertura.xml' -File)
if ($reports.Count -ne 1) {
    throw "Expected exactly one Cobertura report in $ResultsDirectory; found $($reports.Count). Use a fresh results directory."
}

[xml] $coverage = Get-Content -LiteralPath $reports[0].FullName
$packages = @($coverage.coverage.packages.package)
if ($packages.Count -ne 1 -or $packages[0].name -ne 'ExecutionService') {
    throw 'Coverage report must contain the complete ExecutionService production assembly only.'
}

$valid = [int] $coverage.coverage.'lines-valid'
$covered = [int] $coverage.coverage.'lines-covered'
if ($valid -le 0 -or $covered -lt 0 -or $covered -gt $valid) {
    throw "Invalid line counts in coverage report: $covered/$valid"
}

$percent = 100 * $covered / $valid
Write-Output ("ExecutionService line coverage: {0}/{1} ({2:N2}%)" -f $covered, $valid, $percent)
Write-Output "Cobertura: $($reports[0].FullName)"
if ($covered * 100 -lt $valid * 80) {
    throw 'ExecutionService line coverage is below the required 80%.'
}
