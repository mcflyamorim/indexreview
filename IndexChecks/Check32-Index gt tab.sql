/* 
Check32 – Index with size greater than table

Description:
Reports all tables that space to store indexes are greater than the base table size.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review indexes to confirm they are really necessary.

Detailed recommendation:
Review the reported indexes to confirm they are really necessary and are being used.
Check if there are indexes with a high number of include columns and confirm queries are really using all the indexed columns. Avoid adding unnecessary columns in an index, adding too many index columns, key or nonkey, can have performance implications.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck32') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck32
 
SELECT TOP 1000
       'Check32 – Index with size greater than table' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.Indexed_Columns,
       a.Number_Rows AS current_number_of_rows_table,
       CONVERT(NUMERIC(25, 2), a.ReservedSizeInMB / 1024.) AS [Table Size GB],
       CONVERT(NUMERIC(25, 2), ISNULL(t.[Total Index Size],0) / 1024.) AS [Total Index Size GB],
       CASE 
         WHEN CONVERT(NUMERIC(25, 2), a.ReservedSizeInMB / 1024.) < CONVERT(NUMERIC(25, 2), ISNULL(t.[Total Index Size],0) / 1024.) THEN 'Warning - Total index size is greater than the table size.'
         ELSE 'OK'
       END AS [Comment]
  INTO tempdb.dbo.tmpIndexCheck32
  FROM tempdb.dbo.Tab_GetIndexInfo a
  OUTER APPLY (SELECT SUM(ReservedSizeInMB) FROM tempdb.dbo.Tab_GetIndexInfo b 
                WHERE a.Database_Name = b.Database_Name
                  AND a.object_id = b.object_id
                  AND b.Index_Type NOT IN ('HEAP', 'CLUSTERED', 'CLUSTERED COLUMNSTORE')) AS t ([Total Index Size])
 WHERE a.Index_Type IN ('HEAP', 'CLUSTERED', 'CLUSTERED COLUMNSTORE')
ORDER BY a.ReservedSizeInMB DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck32
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name