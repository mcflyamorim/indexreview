/* 
Check3 - Duplicated or overlapped indexes

Description:
Duplicate indexes are indexes with a key identical or overlapped by another index. Like unused indexes, duplicates cost extra resources without providing a benefit.

Estimated Benefit:
High

Estimated Effort:
Very High

Recommendation:
Quick recommendation:
Remove or merge the indexes.

Detailed recommendation:
There is clearly no benefit from having the same index more than once, but on the other hand there is quite a lot of overhead that each index incurs. 
The overhead includes the storage consumed by the index, the on-going maintenance during DML statements, the periodical rebuild and/or reorganize operations, and the additional complexity that the optimizer has to deal with when evaluating possible access methods to the relevant table or view.
Review the duplicate and overlapping indexes. Compare the keys, included columns and constraint to identify which indexes can be merged together and which can be safely dropped.

Note 1: To minimize the risks associated with removing disabled indexes, it is important to carefully evaluate the impact of the removal and perform the necessary testing and validation before making any changes to the database schema.
Note 2: Make sure there are no dependencies of the indexes, like hard-coded indexes in modules.
Note 3: Some applications may require specific indexes to work, in addition, some app vendors will consider environment non-supported if database schema is changed.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck3') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck3

SELECT 'Check 3 - Duplicated indexes' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.included_columns,
       a.filter_definition,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.plan_cache_reference_count,
       CONVERT(XML, (STUFF((SELECT ', ' + QUOTENAME(b.Index_Name)
                FROM tempdb.dbo.Tab_GetIndexInfo AS b 
               WHERE a.Database_ID = b.Database_ID 
                 AND a.Object_ID = b.Object_ID
                 AND a.Index_ID <> b.Index_ID
                 AND CONVERT(VARCHAR(MAX), a.indexed_columns) = CONVERT(VARCHAR(MAX), b.indexed_columns)
                 AND CONVERT(VARCHAR(MAX), a.included_columns) = CONVERT(VARCHAR(MAX), b.included_columns)
                 AND ISNULL(a.filter_definition,'') = ISNULL(b.filter_definition,'')
              FOR XML PATH('')), 1, 2, ''))) AS Exact_Duplicated,
       CONVERT(XML, STUFF((SELECT ', ' + QUOTENAME(b.Index_Name)
                FROM tempdb.dbo.Tab_GetIndexInfo AS b 
               WHERE a.Database_ID = b.Database_ID 
                 AND a.Object_ID = b.Object_ID
                 AND a.Index_ID <> b.Index_ID
                 AND CONVERT(VARCHAR(MAX), b.indexed_columns) LIKE CONVERT(VARCHAR(MAX), a.indexed_columns) + '%'
                 AND ISNULL(a.filter_definition,'') = ISNULL(b.filter_definition,'')
                FOR XML PATH('')), 1, 2, '')) AS Overlapped_Index,
       DropCmd = 
       N'USE ' + QUOTENAME(a.Database_Name) + ';' + NCHAR(13) + NCHAR(10) 
       + 'EXEC sp_helpindex2 ' + '''' + QUOTENAME(a.Schema_Name) + N'.' + QUOTENAME(a.Table_Name) + ''''
       + '/* NumberOfRows = ' + CONVERT(VARCHAR, Number_Rows) + '*/'
       + NCHAR(13) + NCHAR(10) +
       + N'-- DROP INDEX ' + QUOTENAME(a.Index_Name) + N' ON ' + QUOTENAME(a.Schema_Name) + N'.' + QUOTENAME(a.Table_Name) + N';'
       + NCHAR(13) + NCHAR(10) +
       'GO',
       DisableCmd = 
       N'USE ' + QUOTENAME(a.Database_Name) + ';' + NCHAR(13) + NCHAR(10) 
       + 'ALTER INDEX ' + QUOTENAME(a.Index_Name) + N' ON ' + QUOTENAME(a.Schema_Name) + N'.' + QUOTENAME(a.Table_Name) + ' DISABLE;' 
       + NCHAR(13) + NCHAR(10) +
       'GO'
  INTO tempdb.dbo.tmpIndexCheck3
  FROM tempdb.dbo.Tab_GetIndexInfo a

SELECT * FROM tempdb.dbo.tmpIndexCheck3
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         CONVERT(VARCHAR(MAX), indexed_columns),
         ReservedSizeInMB DESC,
         Index_Name
