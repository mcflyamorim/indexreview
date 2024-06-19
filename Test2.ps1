Clear-Host

$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

& "$ScriptPath\ExportIndexChecksToExcel.ps1" -SQLInstance "server1fabianoamorim.database.windows.net,1433" -Database "db1" -LogFilePath "C:\temp\" -Force_sp_GetIndexInfo_Execution -CreateTranscriptLog -ShowVerboseMessages
