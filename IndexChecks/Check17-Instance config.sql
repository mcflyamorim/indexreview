/*
Check17 - Check instance configurations

Description:
Check some instance configurations related to index.

Estimated Benefit:
Medium

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Set index create memory and fill factor (%) options to 0.

Detailed recommendation:
The index create memory and fill factor (%) options are advanced options and should be changed only by an experienced database administrator.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck17') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck17

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT 'Check17 - Check instance configurations' AS [Info],
       *, 
       CASE WHEN value <> 0 THEN 'Warning - Value is not set to default' ELSE 'OK' END AS comment
INTO tempdb.dbo.tmpIndexCheck17
FROM sys.configurations
WHERE name IN ('fill factor (%)', 'index create memory (KB)')

SELECT * FROM tempdb.dbo.tmpIndexCheck17