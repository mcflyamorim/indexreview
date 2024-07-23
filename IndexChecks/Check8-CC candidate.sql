/* 
Check8 - Find Clustered Columnstore Candidate Tables

Description:
A clustered columnstore index (CCI) can be created on one or more tables to improve query 
and data load performance and drastically reduce storage space and memory consumption by the table.

Estimated Benefit:
High

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Create the clustered columnstore index

Detailed recommendation:

Some guidelines used in this script: 

How large is my table?
Each partition has at least a million rows. Columnstore indexes have rowgroups within each partition. 
If the table is too small to fill a rowgroup within each partition, you won't get the benefits of columnstore compression and query performance.
-> I'm only considering tables with at least a million rows.

Do my queries mostly perform analytics that scan large ranges of values? CCI are designed to work well for large range scans rather than looking up specific values.
CCI indexes are not recommended in OLTP workloads, or when queries predominantly use index seeks, lookups, and small range scans.
-> I'm considering that if the index access is mostly via scans, it is a good candidate for CCI. 
-> I'm considering good candidates, all indexes with ratio of scans compared to seeks and lookups is greather than 70%.

Does my workload perform lots of updates and deletes? Columnstore indexes work well when the data is stable. Queries should be updating and deleting less than 10% of the rows.
CCI are not recommended when rows in the table are frequently updated, or when less frequent but large updates occur.
Large numbers of updates and deletes cause fragmentation. 
The fragmentation affects compression rates and query performance until you run an operation called reorganize that forces all data into the columnstore and removes fragmentation.
-> I'm ignoring indexes with ratio of writes(compared to reads) is greather than 10%.

Other important considerations you should evaluate: 
- Most of the inserts are on large volumes of data with minimal updates and deletes. 
  Many workloads such as Internet of Things (IOT) insert large volumes of data with minimal updates and deletes. 
  These workloads can benefit from the compression and query performance gains that come from using a clustered columnstore index.

Note: Index usage statistics for the query workload on each table are included to help determine if CCI will benefit queries referencing this table.
      To improve tip accuracy, obtain this result while a representative workload is running, or shortly thereafter.
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY;

IF OBJECT_ID('dbo.tmpIndexCheck8') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck8;

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* Assumption: a representative workload has populated index operational stats for relevant tables */
;WITH CTE_1
AS
(
SELECT 'Check8 - Find Clustered Columnstore Candidate Tables' AS [Info],
     Database_Name,
     Schema_Name,
     Table_Name,
     Index_Name,
     Number_Rows AS current_number_of_rows_table,
     user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
     last_datetime_obj_was_used,
     plan_cache_reference_count,
     ReservedSizeInMB,
     Buffer_Pool_SpaceUsed_MB,
     Buffer_Pool_FreeSpace_MB,
     CONVERT(NUMERIC(18, 2), (Buffer_Pool_FreeSpace_MB / CASE WHEN Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent,
     partition_number,
     data_compression_desc,
     a.user_seeks + a.user_scans + a.user_lookups AS number_of_index_read,
     a.[Total Writes] AS number_of_index_writes,
     RTRIM(
       CONVERT(
         BIGINT,
         CAST(CASE
                WHEN (a.user_seeks + a.user_scans + a.user_lookups) = 0 THEN 0
                ELSE
                  CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups)) * 100
                  / CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)
                      WHEN 0 THEN 1
                      ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates))
                    END
              END AS DECIMAL(18, 2))))  AS [reads_ratio],
     RTRIM(
       CONVERT(
         BIGINT,
         CAST(CASE
                WHEN a.user_updates = 0 THEN 0
                ELSE
                  CONVERT(REAL, a.user_updates) * 100
                  / CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)
                      WHEN 0 THEN 1
                      ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates))
                    END
              END AS DECIMAL(18, 2)))) AS [writes_ratio],
     a.user_scans AS number_of_index_scans,
     a.user_seeks AS number_of_index_seeks,
     a.user_lookups AS number_of_index_lookups,
     RTRIM(
       CONVERT(
         BIGINT,
         CAST(CASE
                WHEN (a.user_scans) = 0 THEN 0
                ELSE
                  CONVERT(REAL, (a.user_scans)) * 100
                  / CASE (a.user_seeks + a.user_scans + a.user_lookups)
                      WHEN 0 THEN 1
                      ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups))
                    END
              END AS DECIMAL(18, 2)))) AS [scan_ratio_compared_to_seeks_and_lookup],
     a.singleton_lookup_count AS number_of_index_singleton_lookups,
     a.range_scan_count AS number_of_index_range_scans,
     a.singleton_lookup_count + a.range_scan_count AS number_of_singleton_plus_range_scan_access,
     RTRIM(
       CONVERT(
         BIGINT,
         CAST(CASE
                WHEN (a.singleton_lookup_count + a.range_scan_count) = 0 THEN 0
                ELSE
                  CONVERT(REAL, (a.singleton_lookup_count)) * 100
                  / CONVERT(REAL, (a.singleton_lookup_count + a.range_scan_count))
              END AS DECIMAL(18, 2)))) AS [singleton_lookup_ratio],
     RTRIM(
       CONVERT(
         BIGINT,
         CAST(CASE
                WHEN (a.singleton_lookup_count + a.range_scan_count) = 0 THEN 0
                ELSE
                  CONVERT(REAL, (a.range_scan_count)) * 100
                  / CONVERT(REAL, (a.singleton_lookup_count + a.range_scan_count))
              END AS DECIMAL(18, 2)))) AS [range_scan_ratio],
     insert_count	= leaf_insert_count,
     update_count	= leaf_update_count,
     delete_count	= leaf_delete_count + leaf_ghost_count,
     'Based on current index usage stats, a CCI will benefit queries referencing this table.' AS Comment
FROM dbo.Tab_GetIndexInfo AS a
WHERE Index_ID <= 1
/* Ignoring objects that already have a CCI */
AND NOT EXISTS(SELECT 1 AS object_has_columnstore_indexes
                 FROM dbo.Tab_GetIndexInfo AS b 
                 WHERE a.Database_ID = b.Database_ID 
                 AND a.Object_ID = b.OBJECT_ID
                 AND data_compression_desc LIKE '%CLUSTERED COLUMNSTORE%')
)
SELECT * 
INTO dbo.tmpIndexCheck8
FROM CTE_1
WHERE 
(
/* Only considering tables with more than 1mi rows */
current_number_of_rows_table >= 1000000
/* The ratio of access to the index via scan compared to seek and lookups is greather than 70%, which means, most of accesses are doing scans */
AND scan_ratio_compared_to_seeks_and_lookup >= 70
/* The ratio of writes is less than 10%*/
AND writes_ratio <= 10
)

SELECT * FROM dbo.tmpIndexCheck8
ORDER BY current_number_of_rows_table DESC
