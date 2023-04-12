/*
Check 44 - Report detailed information about plan cache

Description:
Report plan cache info

Estimated Benefit:
NA

Estimated Effort:
NA

Recommendation:
Quick recommendation:

Detailed recommendation:
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck44') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck44

SELECT TOP 10000
       'Check 44 - Report detailed information about plan cache' AS [Info],
       *
INTO tempdb.dbo.tmpIndexCheck44
FROM tempdb.dbo.tmpIndexCheckCachePlanData

SELECT * FROM tempdb.dbo.tmpIndexCheck44
ORDER BY query_impact DESC