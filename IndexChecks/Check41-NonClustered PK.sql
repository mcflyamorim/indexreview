/*
Check41 - Heaps with NonClustered PKs with high number of seeks/singleton_lookups on heap

Description:
Check if there are heaps NonClustered PKs doing a high number of seeks/singleton_lookups

Estimated Benefit:
Medium

Estimated Effort:
Medium

Recommendation:
Quick recommendation:
Re-create indexes as clustered.

Detailed recommendation:
If most of reads on PK are doing RID lookups, it may indicate this index (PK) would be better if it was recreated as a clustered index.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck41') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck41

SELECT 'Check 41 - Heaps with NonClustered PKs with high number of seeks/singleton_lookups on heap' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB,
       a.[user_seeks] AS pk_user_seeks,
       c.[user_seeks_all_other_nonclustered],
       b.[user_lookups] AS Heap_user_lookups,
       Tab1.Ratio_Of_NonClusteredSeeks_VS_HeapLookups,
       CASE 
         WHEN CONVERT(DECIMAL(18, 2), REPLACE(Tab1.Ratio_Of_NonClusteredSeeks_VS_HeapLookups, '%', '')) >= 80 
              THEN 'It looks like ' + Tab1.Ratio_Of_NonClusteredSeeks_VS_HeapLookups + 
                   ' of all reads on this NonClustered PKs did a RID lookup on heap, this may indicate PK is a good candidate to be recreated as a clustered index.'
         ELSE 'OK'
       END AS [Comment],
       CASE b.[singleton_lookup_count]
         WHEN 0 THEN '0%'
         ELSE CONVERT(VARCHAR, CAST((a.[singleton_lookup_count] / (ISNULL(b.[singleton_lookup_count], 1) * 1.00)) * 100.0 AS DECIMAL(18, 2)))  + '%' 
       END AS Ratio_Of_NonClusteredSingletonLookups_VS_HeapSingletonLookups,
       a.[range_scan_count], 
       b.[range_scan_count] AS Heap_range_scan_count, 
       a.[singleton_lookup_count],
       b.[singleton_lookup_count] AS Heap_singleton_lookup_count,
       b.[user_seeks] AS Heap_user_seeks,
       a.[user_scans],
       b.[user_scans] AS Heap_user_scans,
       a.[user_lookups],
       CmdToConvertIndexToClustered = N'USE ' + QUOTENAME(a.Database_Name) + N'; ' + 
       + NCHAR(13) + NCHAR(10) +
       '/* NumberOfRows = ' + CONVERT(VARCHAR, a.Number_Rows) + '*/' + 
       + NCHAR(13) + NCHAR(10) +
       'CREATE UNIQUE CLUSTERED INDEX ' + a.Index_Name + ' ON ' + 
       QUOTENAME(a.Schema_Name) + N'.' + QUOTENAME(a.Table_Name) + '(' + CONVERT(VARCHAR(MAX), a.indexed_columns) + ')' 
       + NCHAR(13) + NCHAR(10) +
       'WITH(DROP_EXISTING=ON)'
       + NCHAR(13) + NCHAR(10) +
       'GO'
INTO dbo.tmpIndexCheck41
FROM dbo.Tab_GetIndexInfo a
INNER JOIN dbo.Tab_GetIndexInfo b
ON a.Database_ID = b.Database_ID
AND a.Object_ID = b.Object_ID
AND b.Index_Type = 'HEAP'
CROSS APPLY (SELECT SUM(c.[user_seeks]) AS [user_seeks_all_other_nonclustered]
             FROM dbo.Tab_GetIndexInfo c
             WHERE a.Database_ID = c.Database_ID
             AND a.Object_ID = c.Object_ID
             AND c.Index_Type = 'NONCLUSTERED'
             AND is_primary_key = 0) AS c
CROSS APPLY (SELECT CASE b.[user_lookups]
                      WHEN 0 THEN '0%'
                      ELSE CONVERT(VARCHAR, CAST(((a.[user_seeks] - c.[user_seeks_all_other_nonclustered]) / (ISNULL(b.[user_lookups], 1) * 1.00)) * 100.0 AS DECIMAL(18, 2)))  + '%' 
                    END AS Ratio_Of_NonClusteredSeeks_VS_HeapLookups) AS Tab1
WHERE a.is_primary_key = 1
AND a.Index_Type = 'NONCLUSTERED'
AND a.Number_Rows >= 1000

SELECT * FROM dbo.tmpIndexCheck41
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
