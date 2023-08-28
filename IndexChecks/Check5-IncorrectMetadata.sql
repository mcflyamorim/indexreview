/* 
Check5 - Incorrect metadata

Description:
Reports row count inaccuracies in the catalog views.
These inaccuracies may cause incorrect space usage reports.

Select count always reads the from underlying objects, hence the information will be accurate. 
But, some people may reply on system data to optimize the count operation and will get an incorrect value.

Estimated Benefit:
Low

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Run DBCC UPDATEUSATE WITH COUNT_ROWS.

Detailed recommendation:
Run DBCC UPDATEUSATE WITH COUNT_ROWS to update the row count column with the current count of the number of rows in the table or view.

Notes:
- It is a best practice to run DBCC UPDATEUSAGE after a database migration. This is the most commom cause of 
inaccuracies on system tables. 

- Consider running DBCC UPDATEUSAGE routinely (for example, weekly) only if the database undergoes frequent 
Data Definition Language (DDL) modifications, such as CREATE, ALTER, or DROP statements.

- DBCC UPDATEUSAGE holds a shared lock on the object, hence this may cause a blocking scenario in a highly concurrent environment. 

- DBCC UPDATEUSAGE should be run on an as-needed basis, for example, when you suspect incorrect values are being returned by sp_spaceused. 
DBCC UPDATEUSAGE can take some time to run on large tables or databases.
*/


SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck5') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck5

SELECT 'Check5 - Incorrect metadata' AS [Info],
        a.Database_Name, 
        a.Schema_Name,
        a.Table_Name,
        a.Index_Name, 
        a.Number_Rows AS current_number_of_rows_table, 
        t1.CntDistinctNumberOfRows,
        'DBCC UPDATEUSAGE (' + QUOTENAME(a.Database_Name) + ',' + CONVERT(VARCHAR, a.Object_ID) + ',' + CONVERT(VARCHAR,a.Index_ID) + ') WITH COUNT_ROWS;' AS CommandToFixIt
INTO tempdb.dbo.tmpIndexCheck5
FROM tempdb.dbo.Tab_GetIndexInfo AS a
CROSS APPLY( 
SELECT COUNT(DISTINCT b.Number_Rows) AS CntDistinctNumberOfRows
  FROM tempdb.dbo.Tab_GetIndexInfo AS b 
 WHERE a.Database_ID = b.Database_ID 
   AND a.Object_ID = b.Object_ID
   AND a.Schema_Name = b.Schema_Name
   AND ISNULL(b.filter_definition,'') = '') AS t1
WHERE CntDistinctNumberOfRows > 1
ORDER BY CntDistinctNumberOfRows DESC
GO

SELECT * FROM tempdb.dbo.tmpIndexCheck5
ORDER BY Database_Name,
         Schema_Name,
         Table_Name,
         CntDistinctNumberOfRows