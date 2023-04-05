/* 
Check28 - Indexes with largest index key

Description:
Reports the TOP indexes and number columns in the index key.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review the number of key columns on TOP N indexes.

Detailed recommendation:
Review the number of key columns on TOP N indexes and make sure they’re following the best practices do define an index. It is usually a best practice to define the index key with as few columns as possible and keep the length of the index key short.
Redesign nonclustered indexes with a large index key size so that only columns used for searching and lookups are key columns. Make all other columns that cover the query included nonkey columns. In this way, you will have all columns needed to cover the query, but the index key itself is small and efficient.
Avoid adding unnecessary columns. Adding too many index columns, key or nonkey, can have the performance implications such as: Fewer index rows will fit on a page (which could create I/O increases and reduced cache efficiency); more disk space will be required to store the index;
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck28') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck28

SELECT TOP 1000
       'Check28 - Indexes with largest index key' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.Indexed_Columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.data_compression_desc,
       a.ReservedSizeInMB,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.Buffer_Pool_SpaceUsed_MB,
       LEN(CONVERT(VARCHAR(MAX), indexed_columns)) - LEN(REPLACE(CONVERT(VARCHAR(MAX), indexed_columns), ',', '')) + 1 AS KeyColumnsCount
  INTO tempdb.dbo.tmpIndexCheck28
  FROM tempdb.dbo.Tab_GetIndexInfo a
ORDER BY LEN(CONVERT(VARCHAR(MAX), indexed_columns)) - LEN(REPLACE(CONVERT(VARCHAR(MAX), indexed_columns), ',', '')) + 1 DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck28
ORDER BY KeyColumnsCount DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
