/* 
Check2 - Fragmentation

Description:
In B-tree (rowstore) indexes, fragmentation exists when indexes have pages in which the logical ordering within the index, based on the key values of the index, does not match the physical ordering of index pages.
For queries that read many pages using full or range index scans, heavily fragmented indexes can degrade query performance because additional I/O may be required to read the data required by the query. Instead of a small number of large I/O requests, the query would require a larger number of small I/O requests to read the same amount of data.

Estimated Benefit:
Very High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Run index defragmentation.

Detailed recommendation:
Reduce index fragmentation of biggest and most accessed objects by using one of the following methods:
•	Reorganize an index
•	Rebuild an index
For more details about the best index maintenance strategy that balances potential performance improvements against resource consumption required for maintenance check the following article: https://learn.microsoft.com/en-us/sql/relational-databases/indexes/reorganize-and-rebuild-indexes?view=sql-server-ver16#index-maintenance-methods-reorganize-and-rebuild

Note 1: Check the mentioned article for specific recommendations and details about an index maintenance on SQL Azure DB and SQL Managed Instances. An index maintenance may not be necessary in those environments as the rebuild or reorganize operation may degrade performance of other workloads due to resource contention.

Note 2: If available memory is enough to keep all the database pages in cache, fragmentation may be less important, but it is still important to use the available resources as best as possible and avoid extra storage space caused by the internal fragmentation.

Note 3:
Some important notes about why fragmentation still matters even on most modern storage hardware:
- Reading from memory is still a lot faster than reading from any storage (flash based or not) subsystem.
- Low page density (internal fragmentation) will require more pages to store the data, given the cost per gigabyte for high-end storage this could be quite significant.
- Index fragmentation affects the performance of scans and range scans through limiting the size of read-ahead I/Os. This could result in SQL Server not being able to take full advantage of the IOPS and I/O throughput capacity of the storage subsystem. Depending on the storage capability, SQL Server usually achieves a much higher I/O throughput as a direct consequence of requesting large I/Os, as an example, SQL Server can use read-ahead to do up to 8MB in a single I/O request on SQL EE and ColumnStore. It is definitely more efficient to issue 1 x 8-page read than 8 x 1-page reads.
- Index fragmentation can adversely impact execution plan choice: When the Query Optimizer compiles a query plan, it considers the cost of I/O needed to read the data required by the query. With low page density, there are more pages to read, therefore the cost of I/O is higher. This can impact query plan choice. For example, as page density decreases over time due to page splits, the optimizer may compile a different plan for the same query, with a different performance and resource consumption profile.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck2') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck2

SELECT 'Check 2 - Index fragmentation' AS [Info],
        a.Database_Name,
        a.Schema_Name,
        a.Table_Name,
        a.Index_Name,
        a.Index_Type,
        a.indexed_columns,
        a.Number_Rows AS current_number_of_rows_table,
        a.data_compression_desc,
        user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
        a.last_datetime_obj_was_used,
        a.plan_cache_reference_count,
        a.ReservedSizeInMB,
        a.Buffer_Pool_SpaceUsed_MB,
        a.Buffer_Pool_FreeSpace_MB,
        CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / CASE WHEN a.Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE a.Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent,
        a.fill_factor,
        CONVERT(NUMERIC(18, 2), a.avg_fragmentation_in_percent) AS avg_fragmentation_in_percent,
        CONVERT(NUMERIC(18, 2), a.avg_page_space_used_in_percent) AS avg_page_space_used_in_percent,
        forwarded_record_count, -- for heaps
        CASE 
          WHEN CONVERT(NUMERIC(18, 2), a.avg_fragmentation_in_percent) > 5 THEN 'Warning - This index is fragmented. It is recommended to remove fragmentation on a regular basis to maintain performance'
          ELSE 'OK'
        END AS [Comment 1]
 INTO tempdb.dbo.tmpIndexCheck2
 FROM tempdb.dbo.Tab_GetIndexInfo a

SELECT * FROM tempdb.dbo.tmpIndexCheck2
 ORDER BY avg_fragmentation_in_percent DESC,
          ReservedSizeInMB DESC;
