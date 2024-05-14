/* 
Check35 - Indexes that are not compressed

Description:
SQL Server support row and page compression for rowstore tables and indexes, and support columnstore and columnstore archival compression for columnstore tables and indexes.
In addition to saving space, data compression can help improve performance of I/O intensive workloads because the data is stored in fewer pages and queries need to read fewer pages from disk.

Estimated Benefit:
Very High

Estimated Effort:
Medium

Recommendation:
Quick recommendation:
Consider to enable SQL Server native page, row or columnstore compression on reported indexes.

Detailed recommendation:
Consider to enable SQL Server native page, row or columnstore compression on reported indexes. Compression will help to reduce I/O reads and writes operations and increase buffer pool data cache efficiency.
Watch out for large_value_types_out_of_row equals to 1 as off-row data is not compressed when enabling data compression. For example, an XML record that's larger than 8060 bytes will use out-of-row pages, which are not compressed. 
As an alternative to compress data for those columns, you can consider the following options:
•	Use SQL native COMPRESS function that will use the GZIP algorithm format.
•	Create a CLR function to apply the compression.
•	Create a clustered columnstore index.

Note: Extra CPU resources are required on the database server to compress and decompress the data, while data is exchanged with the application. It is recommended to evaluate the trade-off of reduce storage space vs the extra CPU cost.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck35') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck35

SELECT TOP 1000
       'Check35 - Indexes that are not compressed' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS [Table Size],
       CONVERT(NUMERIC(25, 2), (a.in_row_reserved_page_count * 8) / 1024.) AS in_row_reserved_mb,
       CONVERT(NUMERIC(25, 2), (a.lob_reserved_page_count * 8) / 1024.) AS lob_reserved_mb,
       CONVERT(NUMERIC(25, 2), (a.row_overflow_reserved_page_count * 8) / 1024.) AS row_overflow_reserved_mb,
       a.Buffer_Pool_SpaceUsed_MB,
       a.Buffer_Pool_FreeSpace_MB,
       CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / CASE WHEN a.Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE a.Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent,
       a.page_io_latch_wait_count,
       CAST(1. * a.page_io_latch_wait_in_ms / NULLIF(a.page_io_latch_wait_count ,0) AS DECIMAL(12,2)) AS page_io_latch_avg_wait_ms,
       a.page_io_latch_wait_in_ms AS total_page_io_latch_wait_in_ms,
       CONVERT(VARCHAR(200), ((page_io_latch_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((page_io_latch_wait_in_ms) / 1000), 0), 108) AS total_page_io_latch_wait_d_h_m_s,
       a.TableHasLOB,
       a.large_value_types_out_of_row,
       a.data_compression_desc
  INTO tempdb.dbo.tmpIndexCheck35
  FROM tempdb.dbo.Tab_GetIndexInfo a
 WHERE a.data_compression_desc LIKE '%NONE%'
ORDER BY Number_Rows DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck35
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name