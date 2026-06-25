param(
    [string]$AirflowContainer = "rltm_bi_pltfrm-airflow-scheduler-1",
    [string]$CatalogPath = "configs/batch/pipeline_catalog.json",
    [string]$PipelineName = "",
    [string]$JobName = "",
    [string]$TableId = "",
    [string]$SelectionsJson = "",
    [switch]$DryRun,
    [switch]$SkipDependencies
)

$ErrorActionPreference = "Stop"

$cmd = @(
    "exec",
    $AirflowContainer,
    "python",
    "-m",
    "shared.onboarding.control_plane",
    "--catalog",
    $CatalogPath
)

if (-not [string]::IsNullOrWhiteSpace($SelectionsJson)) {
    if (-not [string]::IsNullOrWhiteSpace($PipelineName) -or
        -not [string]::IsNullOrWhiteSpace($JobName) -or
        -not [string]::IsNullOrWhiteSpace($TableId)) {
        throw "Use either -SelectionsJson or -PipelineName/-JobName/-TableId, not both."
    }

    $cmd += @("--selections-json", $SelectionsJson)
}
else {
    if (-not [string]::IsNullOrWhiteSpace($PipelineName)) {
        $cmd += @("--pipeline-name", $PipelineName)
    }

    if (-not [string]::IsNullOrWhiteSpace($JobName)) {
        if ([string]::IsNullOrWhiteSpace($PipelineName)) {
            throw "-PipelineName is required when -JobName is used."
        }
        $cmd += @("--job-name", $JobName)
    }

    if (-not [string]::IsNullOrWhiteSpace($TableId)) {
        if ([string]::IsNullOrWhiteSpace($JobName)) {
            throw "-JobName is required when -TableId is used."
        }
        $cmd += @("--table-id", $TableId)
    }
}

if ($SkipDependencies) {
    $cmd += "--skip-dependencies"
}

if ($DryRun) {
    $cmd += "--dry-run"
}

Write-Host "Running onboarding:"
Write-Host "docker $($cmd -join ' ')"

docker @cmd

if ($LASTEXITCODE -ne 0) {
    throw "Catalog onboarding failed."
}

Write-Host "Catalog onboarding completed."
