Clear-Host

$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

& "$ScriptPath\ExportIndexChecksToExcel.ps1" -SQLInstance "DELLFABIANO\SQL2019" -Database "Northwind" -LogFilePath "C:\temp\" -Force_sp_GetIndexInfo_Execution
