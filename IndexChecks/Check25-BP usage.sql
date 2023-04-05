/* 
Check25 – Indexes by memory usage

Description:
Measures the amount of memory used in the buffer cache by the largest object (based on the number of pages). It checks the sys.dm_os_buffer_descriptors to identify the object, and returns the relative percentage used. This information is important if you want to monitor what is in the buffer area, or if you are having performance-related disk read problems.
Memory is one of the most important resources for SQL Server, so it’s important to make sure SQL Server is using it efficiently. For example, if 90% of the buffer pool (memory area) is being used to store data from one table, it is important to try to optimize the size of this table to save space for other tables in memory. It is very common for one or two objects to be responsible for using a large amount of the buffer cache. To increase the efficiency of the buffer cache area, these objects may benefit from a schema revision (datatype changes or sparse columns), and are great candidates for compression.

Estimated Benefit:
High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Work to improve memory usage.

Detailed recommendation:
Review the queries using the TOP N indexes and make sure they’re not doing table scan, if necessary, create the needed indexes to avoid it.
Review the buffer pool free space and work to increase page density by defragmenting the index and adjusting the fill-factor.
Apply index compression to increase amount of data we’ll be able to cache using the available memory.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck25') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck25

SELECT TOP 1000
       'Check25 – Indexes by memory usage' AS [Info],
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
       a.Buffer_Pool_FreeSpace_MB,
       CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / a.Buffer_Pool_SpaceUsed_MB) * 100) AS Buffer_Pool_FreeSpace_Percent,
       a.fill_factor,
       CONVERT(NUMERIC(18, 2), a.avg_fragmentation_in_percent) AS avg_fragmentation_in_percent,
       CONVERT(NUMERIC(18, 2), a.avg_page_space_used_in_percent) AS avg_page_space_used_in_percent
  INTO tempdb.dbo.tmpIndexCheck25
  FROM tempdb.dbo.Tab_GetIndexInfo a
WHERE a.Buffer_Pool_SpaceUsed_MB > 0
ORDER BY a.Buffer_Pool_SpaceUsed_MB DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck25
ORDER BY Buffer_Pool_SpaceUsed_MB DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
