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

IF OBJECT_ID('dbo.tmpIndexCheck44') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck44

SELECT TOP 10000
       'Check 44 - Report detailed information about plan cache' AS [Info],
       *
INTO dbo.tmpIndexCheck44
FROM dbo.tmpIndexCheckCachePlanData

SELECT * FROM dbo.tmpIndexCheck44
ORDER BY query_impact DESC