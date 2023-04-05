/* 
Check23 – Index naming convention

Description:
There are different schools of thought on how to name indexes and there is not a single best practice that works for everyone, but it is important to whatever you do, be consistent and avoid to use “test”, “_DTA_”, “missing index” on index name to avoid confusion. The purpose of having a good naming convention is to increase code readability.

Estimated Benefit:
Low

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review index name and if possible, rename it to follow a defined naming convention.

Detailed recommendation:
Before applying indexes from the DTA or a [<Name of Missing Index, sysname,>], it is recommended that the names of indexes be changed to match your organization’s index naming standards.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck23') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck23

SELECT 'Check23 – Index naming convention' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       tab3.avg_of_access_per_minute_based_on_index_usage_dmv,
       Tab1.[Reads_Ratio],
	      Tab2.[Writes_Ratio],
       ISNULL(Number_of_Reads,0) AS [Number of reads], 
       ISNULL([Total Writes],0) AS [Number of writes]
  INTO tempdb.dbo.tmpIndexCheck23
  FROM tempdb.dbo.Tab_GetIndexInfo a
  CROSS APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN (a.user_seeks + a.user_scans + a.user_lookups) = 0 THEN 0 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups)) * 100 /
              		      CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)) END END AS DECIMAL(18,2))),0)) AS Tab1([Reads_Ratio])
  CROSS APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN a.user_updates = 0 THEN 0 ELSE CONVERT(REAL, a.user_updates) * 100 /
		                    CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)) END END AS DECIMAL(18,2))),0)) AS Tab2([Writes_Ratio])
  CROSS APPLY (SELECT CONVERT(VARCHAR(200), user_seeks + user_scans + user_lookups + user_updates / 
                               CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                                 WHEN 0 THEN 1
                                 ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                               END)) AS tab3(avg_of_access_per_minute_based_on_index_usage_dmv)
 WHERE a.Index_Name COLLATE Latin1_General_BIN2 LIKE '%Missing Index%'
    OR (a.Index_Name COLLATE Latin1_General_BIN2 LIKE '%MissingIndex%' AND a.Table_Name COLLATE Latin1_General_BIN2 NOT LIKE '%MissingIndex%')
    OR a.Index_Name COLLATE Latin1_General_BIN2 LIKE '%Test%'
    OR (a.Index_Name COLLATE Latin1_General_BIN2 LIKE '%Backup%' AND a.Table_Name COLLATE Latin1_General_BIN2 NOT LIKE '%Backup%')
    OR a.Index_Name COLLATE Latin1_General_BIN2 LIKE '%_DTA_%'

SELECT * FROM tempdb.dbo.tmpIndexCheck23
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
