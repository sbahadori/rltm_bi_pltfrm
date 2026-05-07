param(
  [string]$Service = "dashboard-api",
  [int]$Tail = 200,
  [switch]$Follow
)

$ErrorActionPreference = "Stop"

$args = @("logs", $Service, "--tail", "$Tail")

if ($Follow) {
  $args += "-f"
}

docker @args