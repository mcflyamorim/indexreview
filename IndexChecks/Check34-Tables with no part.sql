/* 
Check34 – Tables greater than 10mi and not partitioned

Description:
Reports all indexes greater than 10 million rows and are not using partitioning.
There are several performance and manageability benefits of configure partitioning on large tables or indexes.

Estimated Benefit:
Medium

Estimated Effort:
Very High

Recommendation:
Quick recommendation:
Consider to implement a purge/archive strategy or implement partitioning on large tables.

Detailed recommendation:
Review reported tables and implement a purge/archive strategy.
Review reported tables and consider to implement SQL Server native partitioning or partitioned views.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck34') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck34

SELECT 'Check34 – Tables greater than 10mi and not partitioned' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS [Table Size]
  INTO tempdb.dbo.tmpIndexCheck34
  FROM tempdb.dbo.Tab_GetIndexInfo a
 WHERE a.Number_Rows >= 10000000
   AND a.IsTablePartitioned = 0
   AND a.Index_ID <= 1

SELECT * FROM tempdb.dbo.tmpIndexCheck34
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name
