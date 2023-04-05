/* 
Check29 – Tables with more indexes

Description:
Reports the TOP tables and number of indexes. Large numbers of indexes on a table affect the performance of INSERT, UPDATE, DELETE, and MERGE statements because all indexes must be adjusted appropriately as data in the table changes.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review TOP N tables and make sure all indexes are used and required.

Detailed recommendation:
Review the TOP tables and make sure there are no duplicated, overlapped or non-used indexes. Compare the keys, included columns and constraint to identify which indexes can be merged together and which can be safely dropped.
Databases or tables with low update requirements, but large volumes of data can benefit from many nonclustered indexes to improve query performance. So, in some cases, it may be ok to have a table with a large number of indexes.
The selection of the right indexes for a database and its workload is a complex balancing act between query speed and update cost. You will have to determine whether the gains in query performance outweigh the effect to performance during data modification and in additional disk space requirements.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck29') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck29

SELECT TOP 1000
       'Check29 – Tables with more indexes' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.Number_Of_Indexes_On_Table
  INTO tempdb.dbo.tmpIndexCheck29
  FROM tempdb.dbo.Tab_GetIndexInfo a
ORDER BY a.Number_Of_Indexes_On_Table DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck29
ORDER BY Number_Of_Indexes_On_Table DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name