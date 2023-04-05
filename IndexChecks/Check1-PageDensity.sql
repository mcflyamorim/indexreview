/* 
Check1 � Page density

Description:
Low page density (also known as page fullness) can impact on query performance and resource consumption.
Each page in the database can contain a variable number of rows. If rows take all space on a page, page density is 100%. If a page is empty, page density is 0%. If a page with 100% density is split in two pages to accommodate a new row, the density of the two new pages is approximately 50%.
When page density is low, more pages are required to store the same amount of data. This means that more I/O is necessary to read and write this data, and more memory is necessary to cache this data. When memory is limited, fewer pages required by a query will be cached, causing even more disk I/O. Consequently, low page density negatively impacts performance. Also, for queries that read many pages using full or range index scans, low page density can degrade query performance because additional I/O may be required to read the data required by the query. Instead of a small number of large I/O requests, the query would require a larger number of small I/O requests to read the same amount of data.
Low page density may increase the number of intermediate B-tree levels. This moderately increases CPU and I/O cost of finding leaf level pages in index scans and seeks.

Estimated Benefit:
Very High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review indexes and work to increase page density to fir more rows in a page.

Detailed recommendation:
In many workloads, increasing page density results in a positive performance impact on query performance and resource consumption. To increase the page density, you should consider the following items:
�	To avoid lowering page density unnecessarily, we do not recommend setting fill-factor to values other than 100 or 0, except in certain cases for indexes experiencing a high number of page splits.
�	By default, SQL will store data from columns using large data types (varchar(max), nvarchar(max), varbinary(max), xml and etc) directly in the data row, up to a limit of 8000 bytes and as long as the value can fit in a page. This can cause a low page density as less rows would fit in a page. Consider to enable the option �large value types out of row� in a table to make SQL store the LOB data out of row, with a 16-byte pointer to the root page. This usually means more rows will fit per page, which can improve performance of queries that do not directly reference the LOB columns.
�	Consider to use one of native data compression, such as, PAGE, ROW or Columnstore.
�	Avoid fragmentation and low page density by reorganizing the indexes. Reorganizing compacts index pages to make page density equal to the fill-factor of the index.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck1') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck1

SELECT 'Check 1 - Check indexes with small number of rows per page' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.Indexed_Columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.data_compression_desc,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.ReservedSizeInMB,
       a.Buffer_Pool_SpaceUsed_MB,
       a.Buffer_Pool_FreeSpace_MB,
       CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / CASE WHEN a.Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE a.Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent,
       a.TableHasLOB,
       a.row_overflow_fetch_in_pages,
       a.row_overflow_fetch_in_bytes,
       a.large_value_types_out_of_row,
       a.fill_factor,
       CONVERT(NUMERIC(18, 2), a.avg_fragmentation_in_percent) AS avg_fragmentation_in_percent,
       CONVERT(NUMERIC(18, 2), a.avg_page_space_used_in_percent) AS avg_page_space_used_in_percent,
       a.avg_record_size_in_bytes,
       a.min_record_size_in_bytes,
       a.max_record_size_in_bytes,
       a.in_row_data_page_count,
       CONVERT(NUMERIC(18, 2), a.Number_Rows / a.in_row_data_page_count) AS [Avg rows per page],
       CONVERT(NUMERIC(18, 4), (a.Number_Rows / a.in_row_data_page_count) / 8192.) AS PageDensity
  INTO tempdb.dbo.tmpIndexCheck1
  FROM tempdb.dbo.Tab_GetIndexInfo a
 WHERE a.in_row_data_page_count > 0

SELECT * FROM tempdb.dbo.tmpIndexCheck1
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
