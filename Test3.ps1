Clear-Host

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

$instance = "dellfabiano\sql2019"
$FileOutput = "C:\temp\test3.xlsx"

$dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'; 
Write-Warning "[$dt] Starting to read rows"


try{
    $Result = Invoke-SqlCmd -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -Verbose -ErrorAction Stop `
        -Query "SELECT collection_time, session_id, query_plan FROM pythian_log.dbo.WhoIsActiveOut WHERE collection_time = '2023-08-17 21:05:02.740' AND session_id IN (666, 668, 557)"

    #$ResultExcel = $Result | Select-Object * -ExcludeProperty HasErrors, ItemArray, RowError, Table, RowState | Format-Table -AutoSize | Out-String -Width 2147483647 | Out-File -FilePath "C:\temp\test3.txt" -Encoding utf8 -Force

    $xl = $Result | Select-Object * -ExcludeProperty "RowError", "RowState", "Table", "ItemArray", "HasErrors" | `
                            Export-Excel -Path $FileOutput -WorkSheetname "Test3" `
                                        -KillExcel -ClearSheet -TableStyle Medium2 `
                                        -PassThru -Numberformat '#,##0'

    Close-ExcelPackage $xl #-Show

    foreach ($row in $Result)
    {
        if ([string]::IsNullOrEmpty($row.query_plan)) { continue } 

        $row.query_plan `
            | Format-Table -AutoSize -Property * | Out-String -Width 2147483647 | Out-File -FilePath "C:\temp\test3_$($row.session_id).sqlplan" -Encoding utf8 -Force
    }
}
catch 
{
    Write-Warning "Error trying to run the script."
    Write-Warning "ErrorMessage: $($_.Exception.Message)"
}


$dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'; 
Write-Warning "[$dt] Finished to read rows"
