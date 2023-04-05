/* 
Check33 – Row/Page lock disabled

Description:
Reports indexes that have options ALLOW_ROW_LOCKS or ALLOW_PAGE_LOCKS set to OFF (default is ON).
Setting those options to OFF can cause unexpected blocking issues and degrade query performance.

Estimated Benefit:
Very High

Estimated Effort:
Medium

Recommendation:
Quick recommendation:
Set ALLOW_ROW_LOCKS or ALLOW_PAGE_LOCKS to ON.

Detailed recommendation:
Review the reported indexes to confirm that disable ALLOW_ROW_LOCKS or ALLOW_PAGE_LOCKS is really necessary and if not, set it back to ON.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck33') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck33

SELECT 'Check33 – Row/Page lock disabled' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS [Table Size],
       a.allow_row_locks,
       a.allow_page_locks
  INTO tempdb.dbo.tmpIndexCheck33
  FROM tempdb.dbo.Tab_GetIndexInfo a
 WHERE (a.allow_row_locks = 0 OR a.allow_page_locks = 0)
   AND a.Index_Type NOT IN ('CLUSTERED COLUMNSTORE', 'NONCLUSTERED COLUMNSTORE')

SELECT * FROM tempdb.dbo.tmpIndexCheck33
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name
