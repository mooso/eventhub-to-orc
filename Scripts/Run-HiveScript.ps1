Param (
    [Parameter(Mandatory=$true)]
    $Script,
    [Parameter(Mandatory=$true)]
    $ClusterName,
    $ContainerName = $ClusterName,
    $StorageAccount = $(Get-AzureSubscription -Current).CurrentStorageAccountName
)

$ErrorActionPreference = 'Stop'

$StorageContext = New-AzureStorageContext -StorageAccountName $StorageAccount `
  -StorageAccountKey $(Get-AzureStorageKey $StorageAccount).Primary

Function Run-SimpleHdiJob($JobDefinition)
{
  If ($OutputDirectory -ne $null)
  {
    # Delete the output directory if it exists from a previous run
    $OutputBlobs = Get-AzureStorageBlob -Context $StorageContext `
      -Container $ContainerName `
      -Prefix "$OutputDirectory" | Remove-AzureStorageBlob
  }
  # Run the job and wait for it to finish
  $Job = Start-AzureHDInsightJob -Cluster $ClusterName `
    -JobDefinition $JobDefinition
  $Job = Wait-AzureHDInsightJob -Job $Job
  # Get the stderror to get the diagnostics output
  Get-AzureHDInsightJobOutput -Cluster $ClusterName `
    -JobId $Job.JobId -StandardError
  Get-AzureHDInsightJobOutput -Cluster $ClusterName `
    -JobId $Job.JobId -StandardOutput
}

Function Upload-StringToBlob($String, $BlobPath)
{
  $TempFile = "$env:TEMP\ToUpload"
  $String | Out-File -Encoding ascii $TempFile
  $Blob = Set-AzureStorageBlobContent -File $TempFile -Blob $BlobPath `
    -Container $ContainerName -Context $StorageContext -Force
  Remove-Item $TempFile
}

Function Delete-Blob($BlobPath)
{
  Remove-AzureStorageBlob -Blob $BlobPath `
    -Container $ContainerName -Context $StorageContext -Force
}

$ScriptBlobPath = "Queries/TempScript.q"
Upload-StringToBlob $Script $ScriptBlobPath
$HiveJobDefinition = New-AzureHDInsightHiveJobDefinition -QueryFile "/$ScriptBlobPath"
Run-SimpleHdiJob $HiveJobDefinition
Delete-Blob $ScriptBlobPath
