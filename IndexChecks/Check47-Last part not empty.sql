/* 
Check47 - Last partition is not empty

Description:
Last partition is not empty for some partitioned tables. 

Estimated Benefit:
High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Make sure to keep a buffer of several empty partitions at the end of the partition list.

Detailed recommendation:
If the very last partition is not empty, splitting it to create a new partition for new data will be slow and resource-intensive, and will block queries 
accessing the table for the duration of the split operation.

If new partitions are periodically added to accommodate new data, make sure to keep a buffer of several empty partitions at the end of the partition list. 
While a single empty partition is sufficient, multiple empty partitions are preferred. They reduce the risk of data getting into the very last partition 
because of failure to split the last empty partition on time.

The details column provides the list of last several partitions for each table if at least some of them are not empty. 
If the very last partition (i.e. the partition number equal to the total count of partitions) is still empty, act before any data is
 loaded into this partition to split it and create at least one more empty partition at the end of the partition list.

Otherwise, plan and prepare for a time-consuming process of splitting a non-empty partition, to avoid all new data accumulating in the last partition. 
This may require application downtime.

*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY;

IF OBJECT_ID('dbo.tmpIndexCheck47') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck47;

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/* The number of empty partitions at head end considered required */
DECLARE @MinEmptyPartitionCount tinyint = 2

SELECT 'Check47 - Last partition is not empty for some partitioned tables' AS [Info],
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
     @MinEmptyPartitionCount AS number_of_last_partitions_considered,
     t2.number_of_rows_on_last_partitions,
     partition_number,
     data_compression_desc,
     a.user_seeks + a.user_scans + a.user_lookups AS number_of_index_read,
     a.[Total Writes] AS number_of_index_writes,
     'Warning - Last partitions are not empty.' AS Comment
INTO dbo.tmpIndexCheck47
FROM dbo.Tab_GetIndexInfo AS a
OUTER APPLY (SELECT SUM(CONVERT(BIGINT, SUBSTRING(t1.splitdata, CHARINDEX('(', t1.splitdata) + 1, LEN(t1.splitdata) - CHARINDEX('(', t1.splitdata) -1))) AS number_of_rows_on_last_partitions
             FROM (SELECT TOP (@MinEmptyPartitionCount) *
                   FROM (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS RowID,
                                O.splitdata 
                         FROM (SELECT *, CAST('<X>'+replace(F.partition_number,',','</X><X>')+'</X>' as XML) as xmlfilter
                                 FROM dbo.Tab_GetIndexInfo AS F
                               WHERE a.Database_ID = F.Database_ID
                                 AND a.Object_ID = F.Object_ID
                                 AND a.Index_ID = F.Index_ID) AS F1
                         CROSS APPLY (SELECT fdata.D.value('.','varchar(50)') as splitdata 
                                      FROM f1.xmlfilter.nodes('X') as fdata(D)) AS O
                        ) AS t1
                   ORDER BY RowID DESC) AS t1
              ) AS t2
WHERE IsIndexPartitioned = 1
AND t2.number_of_rows_on_last_partitions = 0

SELECT * FROM tmpIndexCheck47 AS a
 ORDER BY current_number_of_rows_table DESC, 
          Database_Name,
          Schema_Name,
          Table_Name,
          ReservedSizeInMB DESC,
          Index_Name
GO
