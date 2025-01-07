/*
Check46 - Non-clustered indexes that are good candidates to become clustered indexes

Description:
Check if there are non-clustered indexes that are good candidates to become clustered indexes.

Estimated Benefit:
Medium

Estimated Effort:
Medium

Recommendation:
Quick recommendation:
Re-create indexes as clustered.

Detailed recommendation:
The non-clustered indexes are considered better compared to the existing clustered/heap indexes when 
the number of user seeks on those indexes is greater than the number of lookups and greater then seeks on 
the related to the table clustered index.
*/



SET NOCOUNT ON;
SET ARITHABORT OFF;
SET ARITHIGNORE ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY;

IF OBJECT_ID('dbo.tmpIndexCheck46') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck46;

SELECT 'Check46 - Non-clustered indexes that are good candidates to become clustered indexes' AS [info],
       a.Database_Name AS database_name,
       a.Schema_Name AS schema_name,
       a.Table_Name AS table_name,
       a.Index_Name AS index_name,
       a.Index_Type AS index_type,
       a.indexed_columns,
       a.included_columns,
       a.last_datetime_obj_was_used,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS reserved_size_in_mb,
       a.Buffer_Pool_SpaceUsed_MB AS buffer_pool_spaceused_mb,
       a.plan_cache_reference_count,
       a.user_seeks + a.user_scans + a.user_lookups AS number_of_index_read,
       a.[Total Writes] AS number_of_index_writes,
       RTRIM(
         CONVERT(
           NVARCHAR(10),
           CAST(CASE
                  WHEN (a.user_seeks + a.user_scans + a.user_lookups) = 0 THEN 0
                  ELSE
                    CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups)) * 100
                    / CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)
                        WHEN 0 THEN 1
                        ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates))
                      END
                END AS DECIMAL(18, 2)))) + '%' AS [reads_ratio],
       RTRIM(
         CONVERT(
           NVARCHAR(10),
           CAST(CASE
                  WHEN a.user_updates = 0 THEN 0
                  ELSE
                    CONVERT(REAL, a.user_updates) * 100
                    / CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)
                        WHEN 0 THEN 1
                        ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates))
                      END
                END AS DECIMAL(18, 2)))) + '%' AS [writes_ratio],
       SUM(a.user_seeks + a.user_scans + a.user_lookups) OVER (PARTITION BY a.Database_ID, a.Object_ID) AS total_number_of_table_read,
       CASE SUM(a.user_seeks + a.user_scans + a.user_lookups) OVER (PARTITION BY a.Database_ID, a.Object_ID)
         WHEN 0 THEN '0%'
         ELSE
           CONVERT(
             VARCHAR,
             CONVERT(
               NUMERIC(18, 2),
               CONVERT(NUMERIC(18, 2), (a.user_seeks + a.user_scans + a.user_lookups))
               / CONVERT(
                   NUMERIC(18, 2),
                   SUM(a.user_seeks + a.user_scans + a.user_lookups) OVER (PARTITION BY a.Database_ID, a.Object_ID))
               * 100.00)) + '%'
       END AS table_read_access_percent_ratio,
       a.[user_seeks] AS number_of_index_seeks,
       b.[user_seeks] AS total_number_of_base_table_seeks,
       CASE SUM(a.user_seeks) OVER (PARTITION BY a.Database_ID, a.Object_ID)
         WHEN 0 THEN '0%'
         ELSE
           CONVERT(
             VARCHAR,
             CONVERT(
               NUMERIC(18, 2),
               CONVERT(NUMERIC(18, 2), (a.user_seeks))
               / CONVERT(NUMERIC(18, 2), SUM(a.user_seeks) OVER (PARTITION BY a.Database_ID, a.Object_ID)) * 100.00))
           + '%'
       END AS index_seek_access_percent_ratio,
       a.[user_scans] AS number_of_index_scans,
       a.[user_lookups] AS number_of_index_lookups,
       a.singleton_lookup_count AS number_of_index_singleton_lookups,
       a.range_scan_count AS number_of_index_range_scans,
       a.singleton_lookup_count + a.range_scan_count AS number_of_singleton_plus_range_scan_access,
       RTRIM(
         CONVERT(
           NVARCHAR(10),
           CAST(CASE
                  WHEN (a.singleton_lookup_count + a.range_scan_count) = 0 THEN 0
                  ELSE
                    CONVERT(REAL, (a.singleton_lookup_count)) * 100
                    / CONVERT(REAL, (a.singleton_lookup_count + a.range_scan_count))
                END AS DECIMAL(18, 2)))) + '%' AS [singleton_lookup_ratio],
       RTRIM(
         CONVERT(
           NVARCHAR(10),
           CAST(CASE
                  WHEN (a.singleton_lookup_count + a.range_scan_count) = 0 THEN 0
                  ELSE
                    CONVERT(REAL, (a.range_scan_count)) * 100
                    / CONVERT(REAL, (a.singleton_lookup_count + a.range_scan_count))
                END AS DECIMAL(18, 2)))) + '%' AS [range_scan_ratio],
       Tab1.ratio_of_index_seeks_vs_base_table_seeks,
       Tab1.diff_factor_of_index_seeks_vs_base_table_seeks,
       Tab2.ratio_of_index_seeks_vs_base_table_lookups,
       CASE
         WHEN a.Index_Type IN ('CLUSTERED', 'HEAP') THEN 'OK'
         /*Ignore recommendations for tables with low (5000, fixed number based on voices in my head) number of lookups*/
         WHEN b.user_lookups < 5000 THEN 'OK'

         /*Ignore recommendations for nonclustered indexes with ratio of seeks vs table lookups lower of 25% (fixed number based on voices in my head)*/
         WHEN CONVERT(DECIMAL(18, 2), REPLACE(Tab2.ratio_of_index_seeks_vs_base_table_lookups, '%', '')) < 25 THEN 'OK'
         /*Only provide recommendations for nonclustered indexes when number of seeks is greater than number of seeks of base table/clustered index*/
         WHEN a.user_seeks > b.user_seeks THEN
           'Index [' + ISNULL(a.Index_Name,'HEAP') + '] was used in '
           + CASE SUM(a.user_seeks) OVER (PARTITION BY a.Database_ID, a.Object_ID)
               WHEN 0 THEN '0%'
               ELSE
                 CONVERT(
                   VARCHAR,
                   CONVERT(
                     NUMERIC(18, 2),
                     CONVERT(NUMERIC(18, 2), (a.user_seeks))
                     / CONVERT(NUMERIC(18, 2), SUM(a.user_seeks) OVER (PARTITION BY a.Database_ID, a.Object_ID))
                     * 100.00)) + '%'
             END + ' of all seeks(' + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, a.user_seeks), 1), '.00', '')
           + ') on this table while the base table index [' + ISNULL(b.Index_Name,'HEAP') + '] was only used '
           + CASE b.user_seeks
               WHEN 0 THEN '0%'
               ELSE
                 CONVERT(
                   VARCHAR,
                   CAST((b.user_seeks
                         / (ISNULL(SUM(a.user_seeks) OVER (PARTITION BY a.Database_ID, a.Object_ID), 1) * 1.00))
                        * 100.0 AS DECIMAL(18, 2))) + '%'
             END + ' of the time with a lower number of seeks('
           + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, b.user_seeks), 1), '.00', '') + '), also, number of lookups('
           + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, b.user_lookups), 1), '.00', '')
           + ') on base table may indicate a high percentage (estimation of '
           + Tab2.ratio_of_index_seeks_vs_base_table_lookups + ') of access to [' + ISNULL(a.Index_Name,'HEAP')
           + '] had to do a lookup to read info about other columns, this may indicate this index is a good candidate to be recreated as a clustered index.'
         ELSE 'OK'
       END AS [comment]
INTO dbo.tmpIndexCheck46
FROM dbo.Tab_GetIndexInfo a
INNER JOIN dbo.Tab_GetIndexInfo b
ON a.Database_ID = b.Database_ID
   AND a.Object_ID = b.Object_ID
   AND b.Index_Type IN ('CLUSTERED', 'HEAP')
CROSS APPLY (SELECT CONVERT(VARCHAR,
                            CAST((a.user_seeks / (CASE
                                                    WHEN ISNULL(b.user_seeks, 1) = 0 THEN 1
                                                    ELSE ISNULL(b.user_seeks, 1)
                                                  END * 1.00)) * 100.0 AS DECIMAL(18, 2))) + '%' AS ratio_of_index_seeks_vs_base_table_seeks,
                    CONVERT(VARCHAR,
                            CAST((a.user_seeks / (CASE
                                                    WHEN ISNULL(b.user_seeks, 1) = 0 THEN 1
                                                    ELSE ISNULL(b.user_seeks, 1)
                                                  END * 1.00)) * 100.0 AS BIGINT) / 100) AS diff_factor_of_index_seeks_vs_base_table_seeks) AS Tab1
CROSS APPLY (SELECT CASE
                      WHEN ISNULL(a.[user_seeks], 0) = 0 THEN '0%'
                      ELSE
                        CONVERT(
                          VARCHAR,
                          CONVERT(
                            NUMERIC(18, 2), ((ISNULL(a.[user_seeks], 0) * 1.00) / ISNULL(CASE WHEN b.[user_lookups] = 0 THEN 1 ELSE b.[user_lookups] END, 1)) * 100.0))
                        + '%'
                    END AS ratio_of_index_seeks_vs_base_table_lookups) AS Tab2
WHERE 1 = 1
      AND a.Number_Rows >= 100 /*Ignoring small tables*/;

SELECT *
FROM dbo.tmpIndexCheck46
ORDER BY current_number_of_rows_table DESC,
         database_name,
         schema_name,
         table_name,
         reserved_size_in_mb DESC,
         index_name;
