Param (
    $ColumnsDefinition,
    $StorageAccount = $(Get-AzureSubscription -Current).CurrentStorageAccountName,
    $ContainerName = 'fromstorm',
    $OutputDirectory,
    $TempDirectory,
    $PartitionsPerOrcExecutor,
    $PartitionsPerSpoutExecutor,
    $TuplesPerCycle,
    $NumberOfWorkers
)

$ErrorActionPreference = 'Stop'

Function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value
    Split-Path $Invocation.MyCommand.Path
}

Function Write-StringToArchive($Archive, $EntryName, $Content)
{
    $Writer = $Archive.CreateEntry($EntryName).Open()
    $StreamWriter = New-Object 'System.IO.StreamWriter' @($Writer, [System.Text.Encoding]::ASCII)
    Try
    {
        $StreamWriter.Write($Content)
    }
    Finally
    {
        $StreamWriter.Close()
    }
}

Function Add-PropertyIfThere($Properties, $Name, $Value)
{
    If ($Value -ne $null)
    {
        $Properties += "
$Name=$Value"
    }
    $Properties
}

# Set paths
$RootDirectory = Split-Path $(Get-ScriptDirectory)
$TargetDirectory = "$RootDirectory\target"
$Version = '0.0.1'
$OriginalJar = "$TargetDirectory\eventhub-to-orc-$Version-jar-with-dependencies.jar"
$TargetJar = "$TargetDirectory\eventhub-to-orc-$Version-prepared.jar"

# Copy the built jar to update it
Copy-Item $OriginalJar $TargetJar

# Open the archive
Add-Type -Assembly System.IO.Compression.FileSystem
$Archive = [System.IO.Compression.ZipFile]::Open($TargetJar, 'Update')
Try
{
    # Update the event hub configuration using the test configuration
    $Archive.GetEntry('Config.properties').Delete()
    $InitialProperties = Get-Content "$RootDirectory\TestConf\EH.properties" -Raw
    $FinalProperties = $InitialProperties + "
storm.orc.columns.definition=$($ColumnsDefinition.Replace("`n", ' '))
"
    $FinalProperties = Add-PropertyIfThere $FinalProperties "storm.orc.output.directory" $OutputDirectory
    $FinalProperties = Add-PropertyIfThere $FinalProperties "storm.orc.temp.directory" $TempDirectory
    $FinalProperties = Add-PropertyIfThere $FinalProperties "storm.orc.partitions.per.orc.executor" $PartitionsPerOrcExecutor
    $FinalProperties = Add-PropertyIfThere $FinalProperties "storm.orc.partitions.per.spout.executor" $PartitionsPerSpoutExecutor
    $FinalProperties = Add-PropertyIfThere $FinalProperties "storm.orc.tuples.per.cycle" $TuplesPerCycle
    $FinalProperties = Add-PropertyIfThere $FinalProperties "storm.orc.num.workers" $NumberOfWorkers

    Write-StringToArchive $Archive "Config.properties" $FinalProperties

    # Create the core-site.xml
    $StorageKey = $(Get-AzureStorageKey $StorageAccount).Primary
    $CoreSite = "<configuration xmlns:xi=""http://www.w3.org/2001/XInclude"">
  <property>
    <name>fs.default.name</name>
	<value>wasb://$ContainerName@$StorageAccount.blob.core.windows.net</value>
  </property>
  <property>
    <name>fs.azure.account.key.$StorageAccount.blob.core.windows.net</name>
	<value>$StorageKey</value>
  </property>
  <property>
    <name>fs.azure.skip.metrics</name>
	<value>true</value>
  </property>  
</configuration>"
    Write-StringToArchive $Archive "core-site.xml" $CoreSite
}
Finally
{
    $Archive.Dispose()
}
