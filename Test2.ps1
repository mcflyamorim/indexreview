Clear-Host

$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

& "$ScriptPath\ExportIndexChecksToExcel.ps1" -SQLInstance "AMORIM-7VQGKX3\SQL2022" -LogFilePath "C:\temp\IndexReview\" -Force_sp_GetIndexInfo_Execution -CreateTranscriptLog -ShowVerboseMessages
