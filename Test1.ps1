Clear-Host

function Transpose-Data{
    param(
        [String[]]$Names,
        [Object[][]]$Data
    )
    for($i = 0;; ++$i){
        $Props = [ordered]@{}
        for($j = 0; $j -lt $Data.Length; ++$j){
            if($i -lt $Data[$j].Length){
                $Props.Add($Names[$j], $Data[$j][$i])
            }
        }
        if(!$Props.get_Count()){
            break
        }
        [PSCustomObject]$Props
    }
}

# Installing SqlServer module...
try {
    if (-not (Get-Module SqlServer -Erroraction Stop)) {
		
        Write-Warning "SqlServer is not installed, trying to install it from Galery"
        $VerbosePreference = "SilentlyContinue"
        Install-Module SqlServer -AllowClobber
        Write-Warning "SqlServer installed successfully"
	}
} catch {
    Write-Warning "Error trying to install SqlServer. Aborting."
	exit
}


$sqlInstance = "dellfabiano\sql2019"
$xEventPath = 'D:\Fabiano\Trabalho\FabricioLima\Clientes\GrupoSoma\Statistics usage analysis\ExtendedEvents auto_stats\DBA_CaptureStatsInfo*.xel'
#$xEventPath = 'C:\temp\DBA_CaptureStatsInfo*.xel'
$Tab = "IF OBJECT_ID('tempdb.dbo.AutoStatsXEvent') IS NOT NULL DROP TABLE tempdb.dbo.AutoStatsXEvent;"
Invoke-Sqlcmd -ServerInstance $sqlInstance -Database "tempdb" -Query $Tab

foreach ($file in Get-ChildItem $xEventPath) {
    $dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'; 
    Write-Warning "[$dt] Starting to read file $file"
    $Result = Read-SqlXEvent -FileName $file | Select-Object Name, TimeStamp, `
                                                @{Name="Fields";Expression={$($_.Fields) | Select-Object Value | ConvertTo-Json -Depth 100}}, `
                                                @{Name="Actions";Expression={$($_.Actions) | Select-Object Values | ConvertTo-Json -Depth 100}}
    
    $dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'; 
    Write-Warning "[$dt] Finished to read $DataXelCount rows on file $file"
    Write-Warning "[$dt] Starting to bulkinsert on SQL table"
    Write-SqlTableData -InputData $Result -SchemaName "dbo" -ServerInstance $sqlInstance -DatabaseName "tempdb" -TableName "AutoStatsXEvent" -Force -Passthru
}
