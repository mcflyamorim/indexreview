/*
Check 43 - Report detailed index usage based on last 60 minutes

Description:
Collecting index usage detailed info for the past 1 hour and reporting detailed information.
This is useful to identify table access patterns and detailed index usage.

Estimated Benefit:
Medium

Estimated Effort:
NA

Recommendation:
Quick recommendation:
Review index usage

Detailed recommendation:
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck43') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck43

DECLARE @Minutes INT = 10 /* By default, capture data for 60 minutes */
DECLARE @Database_ID INT, @Cmd NVARCHAR(MAX), @ErrMsg NVARCHAR(MAX)

IF OBJECT_ID('tempdb.dbo.#tmp_dm_db_index_usage_stats') IS NOT NULL
  DROP TABLE #tmp_dm_db_index_usage_stats

CREATE TABLE #tmp_dm_db_index_usage_stats(
 [captured_datetime] [DATETIME2](7) NOT NULL,
	[database_id] [smallint] NOT NULL,
	[object_id] [int] NOT NULL,
	[index_id] [int] NOT NULL,
	[user_seeks] [bigint] NOT NULL,
	[user_scans] [bigint] NOT NULL,
	[user_lookups] [bigint] NOT NULL,
	[user_updates] [bigint] NOT NULL,
	[last_user_seek] [datetime] NULL,
	[last_user_scan] [datetime] NULL,
	[last_user_lookup] [datetime] NULL,
	[last_user_update] [datetime] NULL,
	[system_seeks] [bigint] NOT NULL,
	[system_scans] [bigint] NOT NULL,
	[system_lookups] [bigint] NOT NULL,
	[system_updates] [bigint] NOT NULL,
	[last_system_seek] [datetime] NULL,
	[last_system_scan] [datetime] NULL,
	[last_system_lookup] [datetime] NULL,
	[last_system_update] [datetime] NULL
)

IF OBJECT_ID('tempdb.dbo.#tmp_dm_db_index_operational_stats') IS NOT NULL
  DROP TABLE #tmp_dm_db_index_operational_stats

CREATE TABLE #tmp_dm_db_index_operational_stats(
 [captured_datetime] [DATETIME2](7) NOT NULL,
	[database_id] [smallint] NULL,
	[object_id] [int] NOT NULL,
	[index_id] [int] NOT NULL,
	[range_scan_count] [bigint] NULL,
	[singleton_lookup_count] [bigint] NULL,
	[page_latch_wait_count] [bigint] NULL,
	[page_io_latch_wait_count] [bigint] NULL,
	[leaf_insert_count] [bigint] NULL,
	[leaf_delete_count] [bigint] NULL,
	[leaf_update_count] [bigint] NULL,
	[forwarded_fetch_count] [bigint] NULL,
	[page_latch_wait_in_ms] [bigint] NULL,
	[leaf_ghost_count] [bigint] NULL,
	[nonleaf_insert_count] [bigint] NULL,
	[nonleaf_delete_count] [bigint] NULL,
	[nonleaf_update_count] [bigint] NULL,
	[leaf_allocation_count] [bigint] NULL,
	[nonleaf_allocation_count] [bigint] NULL,
	[leaf_page_merge_count] [bigint] NULL,
	[nonleaf_page_merge_count] [bigint] NULL,
	[lob_fetch_in_pages] [bigint] NULL,
	[lob_fetch_in_bytes] [bigint] NULL,
	[lob_orphan_create_count] [bigint] NULL,
	[lob_orphan_insert_count] [bigint] NULL,
	[row_overflow_fetch_in_pages] [bigint] NULL,
	[row_overflow_fetch_in_bytes] [bigint] NULL,
	[column_value_push_off_row_count] [bigint] NULL,
	[column_value_pull_in_row_count] [bigint] NULL,
	[row_lock_count] [bigint] NULL,
	[row_lock_wait_count] [bigint] NULL,
	[row_lock_wait_in_ms] [bigint] NULL,
	[page_lock_count] [bigint] NULL,
	[page_lock_wait_count] [bigint] NULL,
	[page_lock_wait_in_ms] [bigint] NULL,
	[index_lock_promotion_attempt_count] [bigint] NULL,
	[index_lock_promotion_count] [bigint] NULL,
	[tree_page_latch_wait_count] [bigint] NULL,
	[tree_page_latch_wait_in_ms] [bigint] NULL,
	[tree_page_io_latch_wait_count] [bigint] NULL,
	[tree_page_io_latch_wait_in_ms] [bigint] NULL,
	[avg_page_latch_wait_in_ms] [numeric](25, 2) NULL,
	[page_io_latch_wait_in_ms] [bigint] NULL,
	[avg_page_io_latch_wait_in_ms] [numeric](25, 2) NULL
)

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

CREATE TABLE #tmp1 (Database_ID INT, Object_ID INT, Index_ID INT, cMin DATETIME, cMax DATETIME)
CREATE UNIQUE CLUSTERED INDEX ix1 ON #tmp1(Database_ID, Object_ID, Index_ID)

DECLARE @i INT = 0, @captured_datetime DATETIME2(7)

WHILE @i < @Minutes
BEGIN
  SET @captured_datetime = SYSDATETIME()
  SET @i = @i + 1

  /*Starting to read data from sys.dm_db_index_usage_stats*/
  BEGIN TRY
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET NOCOUNT ON; SET LOCK_TIMEOUT 1000; /*1  sec*/;
    /* Creating a copy of sys.dm_db_index_usage_stats because this is too slow to access without an index */
    INSERT INTO #tmp_dm_db_index_usage_stats
    SELECT @captured_datetime AS captured_datetime, 
           database_id, 
           object_id, 
           index_id, 
           user_seeks, 
           user_scans, 
           user_lookups, 
           user_updates, 
           last_user_seek, 
           last_user_scan, 
           last_user_lookup, 
           last_user_update,
           system_seeks, 
           system_scans, 
           system_lookups, 
           system_updates, 
           last_system_seek, 
           last_system_scan, 
           last_system_lookup, 
           last_system_update
      FROM sys.dm_db_index_usage_stats AS ius
     WHERE database_id IN (SELECT DISTINCT Database_ID FROM tempdb.dbo.Tab_GetIndexInfo)
    OPTION (RECOMPILE)
  END TRY 
  BEGIN CATCH 
    IF ERROR_NUMBER() = 1222 /*Lock request time out period exceeded.*/
    BEGIN
      SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Got a lock timeout while trying to read data from sys.dm_db_index_usage_stats. You may see limited results because of it.'
      RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
    END
    ELSE
    BEGIN
      SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command ' + CHAR(10) + CHAR(13) + @Cmd
      RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
  
      SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() + ' - ' + CONVERT(VARCHAR(200), ERROR_NUMBER())
      RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
    END
  END CATCH;

  /*Starting to read data from sys.dm_db_index_operational_stats for each database_id*/
  DECLARE c_db CURSOR FAST_FORWARD READ_ONLY FOR
  SELECT DISTINCT Database_ID
  FROM tempdb.dbo.Tab_GetIndexInfo

  OPEN c_db

  FETCH NEXT FROM c_db
  INTO  @Database_ID
  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      SET @Cmd = 
      N'
      SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET NOCOUNT ON; SET LOCK_TIMEOUT 1000; /*1  sec*/;
      /* Creating a copy of sys.dm_db_index_operational_stats because this is too slow to access without an index */
      /* Aggregating the results, to have total for all partitions */
      SELECT @v_captured_datetime AS captured_datetime,
             database_id,
             object_id, 
             index_id,
             SUM(range_scan_count) AS range_scan_count,
             SUM(singleton_lookup_count) AS singleton_lookup_count,
             SUM(page_latch_wait_count) AS page_latch_wait_count,
             SUM(page_io_latch_wait_count) AS page_io_latch_wait_count,
             SUM(leaf_insert_count) AS leaf_insert_count,
             SUM(leaf_delete_count) AS leaf_delete_count,
             SUM(leaf_update_count) AS leaf_update_count,
             SUM(forwarded_fetch_count) AS forwarded_fetch_count,
             SUM(page_latch_wait_in_ms) AS page_latch_wait_in_ms,
             SUM(leaf_ghost_count) AS leaf_ghost_count,
             SUM(nonleaf_insert_count) AS nonleaf_insert_count,
             SUM(nonleaf_delete_count) AS nonleaf_delete_count,
             SUM(nonleaf_update_count) AS nonleaf_update_count,
             SUM(leaf_allocation_count) AS leaf_allocation_count,
             SUM(nonleaf_allocation_count) AS nonleaf_allocation_count,
             SUM(leaf_page_merge_count) AS leaf_page_merge_count,
             SUM(nonleaf_page_merge_count) AS nonleaf_page_merge_count,
             SUM(lob_fetch_in_pages) AS lob_fetch_in_pages,
             SUM(lob_fetch_in_bytes) AS lob_fetch_in_bytes,
             SUM(lob_orphan_create_count) AS lob_orphan_create_count,
             SUM(lob_orphan_insert_count) AS lob_orphan_insert_count,
             SUM(row_overflow_fetch_in_pages) AS row_overflow_fetch_in_pages,
             SUM(row_overflow_fetch_in_bytes) AS row_overflow_fetch_in_bytes,
             SUM(column_value_push_off_row_count) AS column_value_push_off_row_count,
             SUM(column_value_pull_in_row_count) AS column_value_pull_in_row_count,
             SUM(row_lock_count) AS row_lock_count,
             SUM(row_lock_wait_count) AS row_lock_wait_count,
             SUM(row_lock_wait_in_ms) AS row_lock_wait_in_ms,
             SUM(page_lock_count) AS page_lock_count,
             SUM(page_lock_wait_count) AS page_lock_wait_count,
             SUM(page_lock_wait_in_ms) AS page_lock_wait_in_ms,
             SUM(index_lock_promotion_attempt_count) AS index_lock_promotion_attempt_count,
             SUM(index_lock_promotion_count) AS index_lock_promotion_count,
             SUM(tree_page_latch_wait_count) AS tree_page_latch_wait_count,
             SUM(tree_page_latch_wait_in_ms) AS tree_page_latch_wait_in_ms,
             SUM(tree_page_io_latch_wait_count) AS tree_page_io_latch_wait_count,
             SUM(tree_page_io_latch_wait_in_ms) AS tree_page_io_latch_wait_in_ms,
             CONVERT(NUMERIC(25, 2),
             CASE 
               WHEN SUM(page_latch_wait_count) > 0 THEN SUM(page_latch_wait_in_ms) / (1. * SUM(page_latch_wait_count))
               ELSE 0 
             END) AS avg_page_latch_wait_in_ms,
             SUM(page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms,
             CONVERT(NUMERIC(25, 2), 
             CASE 
               WHEN SUM(page_io_latch_wait_count) > 0 THEN SUM(page_io_latch_wait_in_ms) / (1. * SUM(page_io_latch_wait_count))
               ELSE 0 
             END) AS avg_page_io_latch_wait_in_ms
        FROM sys.dm_db_index_operational_stats (@dbid, NULL, NULL, NULL) AS ios
       GROUP BY database_id,
                object_id, 
                index_id
       OPTION (RECOMPILE)
      '
      INSERT INTO #tmp_dm_db_index_operational_stats
      EXEC sp_executesql @Cmd, N'@dbid INT, @v_captured_datetime DATETIME2(7)', @dbid = @Database_ID, @v_captured_datetime = @captured_datetime
    END TRY 
    BEGIN CATCH 
      IF ERROR_NUMBER() = 1222 /*Lock request time out period exceeded.*/
      BEGIN
        SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Got a lock timeout while trying to read data from sys.dm_db_index_operational_stats. You may see limited results because of it.'
        RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
      END
      ELSE
      BEGIN
        SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command ' + CHAR(10) + CHAR(13) + @Cmd
        RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
  
        SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() + ' - ' + CONVERT(VARCHAR(200), ERROR_NUMBER())
        RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
      END
    END CATCH;

    FETCH NEXT FROM c_db
    INTO @Database_ID
  END
  CLOSE c_db
  DEALLOCATE c_db

  /* Wait for 1 minute between each pool */
  WAITFOR DELAY '00:01:00.000'
  --WAITFOR DELAY '00:00:01.000'
END

CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_usage_stats (database_id, object_id, index_id, captured_datetime)
CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_operational_stats (database_id, object_id, index_id, captured_datetime)

--SELECT * FROM #tmp_dm_db_index_usage_stats
--WHERE object_id = 110623437
--AND index_id = 1
--SELECT * FROM #tmp_dm_db_index_operational_stats
--WHERE object_id = 110623437
--AND index_id = 1

;WITH CTE_1
AS
(
SELECT ios.captured_datetime,
       ios.database_id,
       ios.object_id,
       ios.index_id,
       ius.user_seeks - LAG(ius.user_seeks, 1, ius.user_seeks) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS user_seeks,
       ius.user_scans - LAG(ius.user_scans, 1, ius.user_scans) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS user_scans,
       ius.user_lookups - LAG(ius.user_lookups, 1, ius.user_lookups) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS user_lookups,
       ius.user_updates - LAG(ius.user_updates, 1, ius.user_updates) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS user_updates,
       ius.last_user_seek,
       ius.last_user_scan,
       ius.last_user_lookup,
       ius.last_user_update,
       ius.system_seeks - LAG(ius.system_seeks, 1, ius.system_seeks) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS system_seeks,
       ius.system_scans - LAG(ius.system_scans, 1, ius.system_scans) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS system_scans,
       ius.system_lookups - LAG(ius.system_lookups, 1, ius.system_lookups) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS system_lookups,
       ius.system_updates - LAG(ius.system_updates, 1, ius.system_updates) OVER(PARTITION BY ius.database_id, ius.object_id, ius.index_id ORDER BY ius.captured_datetime) AS system_updates,
       ius.last_system_seek,
       ius.last_system_scan,
       ius.last_system_lookup,
       ius.last_system_update,
       ios.range_scan_count - LAG(ios.range_scan_count, 1, ios.range_scan_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS range_scan_count,
       ios.singleton_lookup_count - LAG(ios.singleton_lookup_count, 1, ios.singleton_lookup_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS singleton_lookup_count,
       ios.page_latch_wait_count - LAG(ios.page_latch_wait_count, 1, ios.page_latch_wait_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_latch_wait_count,
       ios.page_io_latch_wait_count - LAG(ios.page_io_latch_wait_count, 1, ios.page_io_latch_wait_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_io_latch_wait_count,
       ios.leaf_insert_count - LAG(ios.leaf_insert_count, 1, ios.leaf_insert_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS leaf_insert_count,
       ios.leaf_delete_count - LAG(ios.leaf_delete_count, 1, ios.leaf_delete_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS leaf_delete_count,
       ios.leaf_update_count - LAG(ios.leaf_update_count, 1, ios.leaf_update_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS leaf_update_count,
       ios.forwarded_fetch_count - LAG(ios.forwarded_fetch_count, 1, ios.forwarded_fetch_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS forwarded_fetch_count,
       ios.page_latch_wait_in_ms - LAG(ios.page_latch_wait_in_ms, 1, ios.page_latch_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_latch_wait_in_ms,
       ios.leaf_ghost_count - LAG(ios.leaf_ghost_count, 1, ios.leaf_ghost_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS leaf_ghost_count,
       ios.nonleaf_insert_count - LAG(ios.nonleaf_insert_count, 1, ios.nonleaf_insert_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS nonleaf_insert_count,
       ios.nonleaf_delete_count - LAG(ios.nonleaf_delete_count, 1, ios.nonleaf_delete_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS nonleaf_delete_count,
       ios.nonleaf_update_count - LAG(ios.nonleaf_update_count, 1, ios.nonleaf_update_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS nonleaf_update_count,
       ios.leaf_allocation_count - LAG(ios.leaf_allocation_count, 1, ios.leaf_allocation_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS leaf_allocation_count,
       ios.nonleaf_allocation_count - LAG(ios.nonleaf_allocation_count, 1, ios.nonleaf_allocation_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS nonleaf_allocation_count,
       ios.leaf_page_merge_count - LAG(ios.leaf_page_merge_count, 1, ios.leaf_page_merge_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS leaf_page_merge_count,
       ios.nonleaf_page_merge_count - LAG(ios.nonleaf_page_merge_count, 1, ios.nonleaf_page_merge_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS nonleaf_page_merge_count,
       ios.lob_fetch_in_pages - LAG(ios.lob_fetch_in_pages, 1, ios.lob_fetch_in_pages) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS lob_fetch_in_pages,
       ios.lob_fetch_in_bytes - LAG(ios.lob_fetch_in_bytes, 1, ios.lob_fetch_in_bytes) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS lob_fetch_in_bytes,
       ios.lob_orphan_create_count - LAG(ios.lob_orphan_create_count, 1, ios.lob_orphan_create_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS lob_orphan_create_count,
       ios.lob_orphan_insert_count - LAG(ios.lob_orphan_insert_count, 1, ios.lob_orphan_insert_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS lob_orphan_insert_count,
       ios.row_overflow_fetch_in_pages - LAG(ios.row_overflow_fetch_in_pages, 1, ios.row_overflow_fetch_in_pages) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS row_overflow_fetch_in_pages,
       ios.row_overflow_fetch_in_bytes - LAG(ios.row_overflow_fetch_in_bytes, 1, ios.row_overflow_fetch_in_bytes) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS row_overflow_fetch_in_bytes,
       ios.column_value_push_off_row_count - LAG(ios.column_value_push_off_row_count, 1, ios.column_value_push_off_row_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS column_value_push_off_row_count,
       ios.column_value_pull_in_row_count - LAG(ios.column_value_pull_in_row_count, 1, ios.column_value_pull_in_row_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS column_value_pull_in_row_count,
       ios.row_lock_count - LAG(ios.row_lock_count, 1, ios.row_lock_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS row_lock_count,
       ios.row_lock_wait_count - LAG(ios.row_lock_wait_count, 1, ios.row_lock_wait_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS row_lock_wait_count,
       ios.row_lock_wait_in_ms - LAG(ios.row_lock_wait_in_ms, 1, ios.row_lock_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS row_lock_wait_in_ms,
       ios.page_lock_count - LAG(ios.page_lock_count, 1, ios.page_lock_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_lock_count,
       ios.page_lock_wait_count - LAG(ios.page_lock_wait_count, 1, ios.page_lock_wait_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_lock_wait_count,
       ios.page_lock_wait_in_ms - LAG(ios.page_lock_wait_in_ms, 1, ios.page_lock_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_lock_wait_in_ms,
       ios.index_lock_promotion_attempt_count - LAG(ios.index_lock_promotion_attempt_count, 1, ios.index_lock_promotion_attempt_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS index_lock_promotion_attempt_count,
       ios.index_lock_promotion_count - LAG(ios.index_lock_promotion_count, 1, ios.index_lock_promotion_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS index_lock_promotion_count,
       ios.tree_page_latch_wait_count - LAG(ios.tree_page_latch_wait_count, 1, ios.tree_page_latch_wait_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS tree_page_latch_wait_count,
       ios.tree_page_latch_wait_in_ms - LAG(ios.tree_page_latch_wait_in_ms, 1, ios.tree_page_latch_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS tree_page_latch_wait_in_ms,
       ios.tree_page_io_latch_wait_count - LAG(ios.tree_page_io_latch_wait_count, 1, ios.tree_page_io_latch_wait_count) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS tree_page_io_latch_wait_count,
       ios.tree_page_io_latch_wait_in_ms - LAG(ios.tree_page_io_latch_wait_in_ms, 1, ios.tree_page_io_latch_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS tree_page_io_latch_wait_in_ms,
       ios.avg_page_latch_wait_in_ms - LAG(ios.avg_page_latch_wait_in_ms, 1, ios.avg_page_latch_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS avg_page_latch_wait_in_ms,
       ios.page_io_latch_wait_in_ms - LAG(ios.page_io_latch_wait_in_ms, 1, ios.page_io_latch_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS page_io_latch_wait_in_ms,
       ios.avg_page_io_latch_wait_in_ms - LAG(ios.avg_page_io_latch_wait_in_ms, 1, ios.avg_page_io_latch_wait_in_ms) OVER(PARTITION BY ios.database_id, ios.object_id, ios.index_id ORDER BY ios.captured_datetime) AS avg_page_io_latch_wait_in_ms
FROM #tmp_dm_db_index_operational_stats AS ios
LEFT OUTER JOIN #tmp_dm_db_index_usage_stats AS ius
  ON ios.captured_datetime = ius.captured_datetime
 AND ios.database_id = ius.database_id
 AND ios.object_id = ius.object_id
 AND ios.index_id = ius.index_id
WHERE 1=1
),
CTE_2
AS
(
SELECT CTE_1.captured_datetime,
       CTE_1.database_id,
       CTE_1.object_id,
       CTE_1.index_id,
       CASE WHEN CTE_1.user_seeks                    < 0 THEN 0 ELSE CTE_1.user_seeks                   END AS user_seeks,
       CASE WHEN CTE_1.user_scans                    < 0 THEN 0 ELSE CTE_1.user_scans                   END AS user_scans,
       CASE WHEN CTE_1.user_lookups                  < 0 THEN 0 ELSE CTE_1.user_lookups                 END AS user_lookups,
       CASE WHEN CTE_1.user_updates                  < 0 THEN 0 ELSE CTE_1.user_updates                 END AS user_updates,
       last_user_seek,
       last_user_scan,
       last_user_lookup,
       last_user_update,
       CASE WHEN CTE_1.system_seeks                    < 0 THEN 0 ELSE CTE_1.system_seeks                   END AS system_seeks,
       CASE WHEN CTE_1.system_scans                    < 0 THEN 0 ELSE CTE_1.system_scans                   END AS system_scans,
       CASE WHEN CTE_1.system_lookups                  < 0 THEN 0 ELSE CTE_1.system_lookups                 END AS system_lookups,
       CASE WHEN CTE_1.system_updates                  < 0 THEN 0 ELSE CTE_1.system_updates                 END AS system_updates,
       last_system_seek,
       last_system_scan,
       last_system_lookup,
       last_system_update,
       CASE WHEN CTE_1.range_scan_count                    < 0 THEN 0 ELSE CTE_1.range_scan_count                   END AS range_scan_count,
       CASE WHEN CTE_1.singleton_lookup_count              < 0 THEN 0 ELSE CTE_1.singleton_lookup_count             END AS singleton_lookup_count,
       CASE WHEN CTE_1.page_latch_wait_count               < 0 THEN 0 ELSE CTE_1.page_latch_wait_count              END AS page_latch_wait_count,
       CASE WHEN CTE_1.page_io_latch_wait_count            < 0 THEN 0 ELSE CTE_1.page_io_latch_wait_count           END AS page_io_latch_wait_count,
       CASE WHEN CTE_1.leaf_insert_count                   < 0 THEN 0 ELSE CTE_1.leaf_insert_count                  END AS leaf_insert_count,
       CASE WHEN CTE_1.leaf_delete_count                   < 0 THEN 0 ELSE CTE_1.leaf_delete_count                  END AS leaf_delete_count,
       CASE WHEN CTE_1.leaf_update_count                   < 0 THEN 0 ELSE CTE_1.leaf_update_count                  END AS leaf_update_count,
       CASE WHEN CTE_1.forwarded_fetch_count               < 0 THEN 0 ELSE CTE_1.forwarded_fetch_count              END AS forwarded_fetch_count,
       CASE WHEN CTE_1.page_latch_wait_in_ms               < 0 THEN 0 ELSE CTE_1.page_latch_wait_in_ms              END AS page_latch_wait_in_ms,
       CASE WHEN CTE_1.leaf_ghost_count                    < 0 THEN 0 ELSE CTE_1.leaf_ghost_count                   END AS leaf_ghost_count,
       CASE WHEN CTE_1.nonleaf_insert_count                < 0 THEN 0 ELSE CTE_1.nonleaf_insert_count               END AS nonleaf_insert_count,
       CASE WHEN CTE_1.nonleaf_delete_count                < 0 THEN 0 ELSE CTE_1.nonleaf_delete_count               END AS nonleaf_delete_count,
       CASE WHEN CTE_1.nonleaf_update_count                < 0 THEN 0 ELSE CTE_1.nonleaf_update_count               END AS nonleaf_update_count,
       CASE WHEN CTE_1.leaf_allocation_count               < 0 THEN 0 ELSE CTE_1.leaf_allocation_count              END AS leaf_allocation_count,
       CASE WHEN CTE_1.nonleaf_allocation_count            < 0 THEN 0 ELSE CTE_1.nonleaf_allocation_count           END AS nonleaf_allocation_count,
       CASE WHEN CTE_1.leaf_page_merge_count               < 0 THEN 0 ELSE CTE_1.leaf_page_merge_count              END AS leaf_page_merge_count,
       CASE WHEN CTE_1.nonleaf_page_merge_count            < 0 THEN 0 ELSE CTE_1.nonleaf_page_merge_count           END AS nonleaf_page_merge_count,
       CASE WHEN CTE_1.lob_fetch_in_pages                  < 0 THEN 0 ELSE CTE_1.lob_fetch_in_pages                 END AS lob_fetch_in_pages,
       CASE WHEN CTE_1.lob_fetch_in_bytes                  < 0 THEN 0 ELSE CTE_1.lob_fetch_in_bytes                 END AS lob_fetch_in_bytes,
       CASE WHEN CTE_1.lob_orphan_create_count             < 0 THEN 0 ELSE CTE_1.lob_orphan_create_count            END AS lob_orphan_create_count,
       CASE WHEN CTE_1.lob_orphan_insert_count             < 0 THEN 0 ELSE CTE_1.lob_orphan_insert_count            END AS lob_orphan_insert_count,
       CASE WHEN CTE_1.row_overflow_fetch_in_pages         < 0 THEN 0 ELSE CTE_1.row_overflow_fetch_in_pages        END AS row_overflow_fetch_in_pages,
       CASE WHEN CTE_1.row_overflow_fetch_in_bytes         < 0 THEN 0 ELSE CTE_1.row_overflow_fetch_in_bytes        END AS row_overflow_fetch_in_bytes,
       CASE WHEN CTE_1.column_value_push_off_row_count     < 0 THEN 0 ELSE CTE_1.column_value_push_off_row_count    END AS column_value_push_off_row_count,
       CASE WHEN CTE_1.column_value_pull_in_row_count      < 0 THEN 0 ELSE CTE_1.column_value_pull_in_row_count     END AS column_value_pull_in_row_count,
       CASE WHEN CTE_1.row_lock_count                      < 0 THEN 0 ELSE CTE_1.row_lock_count                     END AS row_lock_count,
       CASE WHEN CTE_1.row_lock_wait_count                 < 0 THEN 0 ELSE CTE_1.row_lock_wait_count                END AS row_lock_wait_count,
       CASE WHEN CTE_1.row_lock_wait_in_ms                 < 0 THEN 0 ELSE CTE_1.row_lock_wait_in_ms                END AS row_lock_wait_in_ms,
       CASE WHEN CTE_1.page_lock_count                     < 0 THEN 0 ELSE CTE_1.page_lock_count                    END AS page_lock_count,
       CASE WHEN CTE_1.page_lock_wait_count                < 0 THEN 0 ELSE CTE_1.page_lock_wait_count               END AS page_lock_wait_count,
       CASE WHEN CTE_1.page_lock_wait_in_ms                < 0 THEN 0 ELSE CTE_1.page_lock_wait_in_ms               END AS page_lock_wait_in_ms,
       CASE WHEN CTE_1.index_lock_promotion_attempt_count  < 0 THEN 0 ELSE CTE_1.index_lock_promotion_attempt_count END AS index_lock_promotion_attempt_count,
       CASE WHEN CTE_1.index_lock_promotion_count          < 0 THEN 0 ELSE CTE_1.index_lock_promotion_count         END AS index_lock_promotion_count,
       CASE WHEN CTE_1.tree_page_latch_wait_count          < 0 THEN 0 ELSE CTE_1.tree_page_latch_wait_count         END AS tree_page_latch_wait_count,
       CASE WHEN CTE_1.tree_page_latch_wait_in_ms          < 0 THEN 0 ELSE CTE_1.tree_page_latch_wait_in_ms         END AS tree_page_latch_wait_in_ms,
       CASE WHEN CTE_1.tree_page_io_latch_wait_count       < 0 THEN 0 ELSE CTE_1.tree_page_io_latch_wait_count      END AS tree_page_io_latch_wait_count,
       CASE WHEN CTE_1.tree_page_io_latch_wait_in_ms       < 0 THEN 0 ELSE CTE_1.tree_page_io_latch_wait_in_ms      END AS tree_page_io_latch_wait_in_ms,
       CASE WHEN CTE_1.avg_page_latch_wait_in_ms           < 0 THEN 0 ELSE CTE_1.avg_page_latch_wait_in_ms          END AS avg_page_latch_wait_in_ms,
       CASE WHEN CTE_1.page_io_latch_wait_in_ms            < 0 THEN 0 ELSE CTE_1.page_io_latch_wait_in_ms           END AS page_io_latch_wait_in_ms,
       CASE WHEN CTE_1.avg_page_io_latch_wait_in_ms        < 0 THEN 0 ELSE CTE_1.avg_page_io_latch_wait_in_ms       END AS avg_page_io_latch_wait_in_ms
FROM CTE_1
)
SELECT 'Check 43 - Report detailed index usage based on last 60 minutes' AS [Info],
       ISNULL(a.Database_Name, CTE_2.database_id) AS Database_Name,
       ISNULL(a.Schema_Name, '0') AS Schema_Name,
       ISNULL(a.Table_Name, CTE_2.object_id) AS Table_Name,
       ISNULL(a.Index_Name, CTE_2.index_id) AS Index_Name,
       a.Index_Type,
       a.Number_Rows AS current_number_of_rows_table,
       a.last_datetime_obj_was_used,
       a.ReservedSizeInMB,
       a.Buffer_Pool_SpaceUsed_MB,
       a.plan_cache_reference_count,
       CONVERT(NUMERIC(18, 2), a.avg_fragmentation_in_percent) AS avg_fragmentation_in_percent,
       CONVERT(NUMERIC(18, 2), a.avg_page_space_used_in_percent) AS avg_page_space_used_in_percent,
       COUNT(*) AS number_of_pooled_samples,
       /*Index usage info*/
       SUM(CTE_2.user_seeks) AS user_seeks,
       MIN(CTE_2.user_seeks) AS user_seeks_min,
       MAX(CTE_2.user_seeks) AS user_seeks_max,
       AVG(CTE_2.user_seeks) AS user_seeks_avg,
       SUM(CTE_2.user_scans) AS user_scans,
       MIN(CTE_2.user_scans) AS user_scans_min,
       MAX(CTE_2.user_scans) AS user_scans_max,
       AVG(CTE_2.user_scans) AS user_scans_avg,
       SUM(CTE_2.user_lookups) AS user_lookups,
       MIN(CTE_2.user_lookups) AS user_lookups_min,
       MAX(CTE_2.user_lookups) AS user_lookups_max,
       AVG(CTE_2.user_lookups) AS user_lookups_avg,
       SUM(CTE_2.user_updates) AS user_updates,
       MIN(CTE_2.user_updates) AS user_updates_min,
       MAX(CTE_2.user_updates) AS user_updates_max,
       AVG(CTE_2.user_updates) AS user_updates_avg,
       SUM(CTE_2.system_seeks) AS system_seeks,
       MIN(CTE_2.system_seeks) AS system_seeks_min,
       MAX(CTE_2.system_seeks) AS system_seeks_max,
       AVG(CTE_2.system_seeks) AS system_seeks_avg,
       SUM(CTE_2.system_scans) AS system_scans,
       MIN(CTE_2.system_scans) AS system_scans_min,
       MAX(CTE_2.system_scans) AS system_scans_max,
       AVG(CTE_2.system_scans) AS system_scans_avg,
       SUM(CTE_2.system_lookups) AS system_lookups,
       MIN(CTE_2.system_lookups) AS system_lookups_min,
       MAX(CTE_2.system_lookups) AS system_lookups_max,
       AVG(CTE_2.system_lookups) AS system_lookups_avg,
       SUM(CTE_2.system_updates) AS system_updates,
       MIN(CTE_2.system_updates) AS system_updates_min,
       MAX(CTE_2.system_updates) AS system_updates_max,
       AVG(CTE_2.system_updates) AS system_updates_avg,
       MAX(CTE_2.last_user_seek)     AS last_datetime_user_seek,
       MAX(CTE_2.last_user_scan)     AS last_datetime_user_scan,
       MAX(CTE_2.last_user_lookup)   AS last_datetime_user_lookup,
       MAX(CTE_2.last_user_update)  AS last_datetime_user_update,
       MAX(CTE_2.last_system_seek)   AS last_datetime_system_seek,
       MAX(CTE_2.last_system_scan)   AS last_datetime_system_scan,
       MAX(CTE_2.last_system_lookup) AS last_datetime_system_lookup,
       MAX(CTE_2.last_system_update) AS last_datetime_system_update,
       /*Range Scan*/
       SUM(CTE_2.range_scan_count) AS range_scan,
       MIN(CTE_2.range_scan_count) AS range_scan_min,
       MAX(CTE_2.range_scan_count) AS range_scan_max,
       AVG(CTE_2.range_scan_count) AS range_scan_avg,
       /*Singleton lookup*/
       SUM(CTE_2.singleton_lookup_count) AS singleton_lookup,
       MIN(CTE_2.singleton_lookup_count) AS singleton_lookup_min,
       MAX(CTE_2.singleton_lookup_count) AS singleton_lookup_max,
       AVG(CTE_2.singleton_lookup_count) AS singleton_lookup_avg,
       /*Leaf insert/delete/update*/
       SUM(CTE_2.leaf_insert_count) AS leaf_insert,
       MIN(CTE_2.leaf_insert_count) AS leaf_insert_min,
       MAX(CTE_2.leaf_insert_count) AS leaf_insert_max,
       AVG(CTE_2.leaf_insert_count) AS leaf_insert_avg,
       SUM(CTE_2.leaf_delete_count) AS leaf_delete,
       MIN(CTE_2.leaf_delete_count) AS leaf_delete_min,
       MAX(CTE_2.leaf_delete_count) AS leaf_delete_max,
       AVG(CTE_2.leaf_delete_count) AS leaf_delete_avg,
       SUM(CTE_2.leaf_update_count) AS leaf_update,
       MIN(CTE_2.leaf_update_count) AS leaf_update_min,
       MAX(CTE_2.leaf_update_count) AS leaf_update_max,
       AVG(CTE_2.leaf_update_count) AS leaf_update_avg,
       /*Page Latch*/
       SUM(CTE_2.page_latch_wait_count) AS page_latch_wait,
       MIN(CTE_2.page_latch_wait_count) AS page_latch_wait_min,
       MAX(CTE_2.page_latch_wait_count) AS page_latch_wait_max,
       AVG(CTE_2.page_latch_wait_count) AS page_latch_wait_avg,
       SUM(CTE_2.page_latch_wait_in_ms) AS page_latch_wait_in_ms,
       CONVERT(VARCHAR(200), (SUM(CTE_2.page_latch_wait_in_ms) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(CTE_2.page_latch_wait_in_ms) / 1000), 0), 108) AS page_latch_wait_time_d_h_m_s,
       MIN(CTE_2.page_latch_wait_in_ms) AS page_latch_wait_in_ms_min,
       MAX(CTE_2.page_latch_wait_in_ms) AS page_latch_wait_in_ms_max,
       AVG(CTE_2.page_latch_wait_in_ms) AS page_latch_wait_in_ms_avg,
       AVG(CTE_2.avg_page_latch_wait_in_ms) AS page_latch_avg_per_wait_in_ms,
       /*Page Io Latch*/
       SUM(CTE_2.page_io_latch_wait_count) AS page_io_latch_wait,
       MIN(CTE_2.page_io_latch_wait_count) AS page_io_latch_wait_min,
       MAX(CTE_2.page_io_latch_wait_count) AS page_io_latch_wait_max,
       AVG(CTE_2.page_io_latch_wait_count) AS page_io_latch_wait_avg,
       SUM(CTE_2.page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms,
       CONVERT(VARCHAR(200), (SUM(CTE_2.page_io_latch_wait_in_ms) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(CTE_2.page_io_latch_wait_in_ms) / 1000), 0), 108) AS page_io_latch_wait_time_d_h_m_s,
       MIN(CTE_2.page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms_min,
       MAX(CTE_2.page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms_max,
       AVG(CTE_2.page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms_avg,
       AVG(CTE_2.avg_page_io_latch_wait_in_ms) AS page_io_latch_avg_per_wait_in_ms,
       /*Row Lock*/
       SUM(CTE_2.row_lock_count) AS row_lock,
       MIN(CTE_2.row_lock_count) AS row_lock_min,
       MAX(CTE_2.row_lock_count) AS row_lock_max,
       AVG(CTE_2.row_lock_count) AS row_lock_avg,
       SUM(CTE_2.row_lock_wait_count) AS row_lock_wait,
       MIN(CTE_2.row_lock_wait_count) AS row_lock_wait_min,
       MAX(CTE_2.row_lock_wait_count) AS row_lock_wait_max,
       AVG(CTE_2.row_lock_wait_count) AS row_lock_wait_avg,
       SUM(CTE_2.row_lock_wait_in_ms) AS row_lock_wait_in_ms,
       CONVERT(VARCHAR(200), (SUM(CTE_2.row_lock_wait_in_ms) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(CTE_2.row_lock_wait_in_ms) / 1000), 0), 108) AS row_lock_wait_time_d_h_m_s,
       MIN(CTE_2.row_lock_wait_in_ms) AS row_lock_wait_in_ms_min,
       MAX(CTE_2.row_lock_wait_in_ms) AS row_lock_wait_in_ms_max,
       AVG(CTE_2.row_lock_wait_in_ms) AS row_lock_wait_in_ms_avg,
       CONVERT(NUMERIC(25, 2), 
       CASE 
         WHEN SUM(CTE_2.row_lock_count) > 0 THEN SUM(CTE_2.row_lock_wait_in_ms) / (1. * SUM(CTE_2.row_lock_count))
         ELSE 0 
       END) AS row_lock_avg_per_wait_in_ms,
       /*Page Lock*/
       SUM(CTE_2.page_lock_count) AS page_lock,
       MIN(CTE_2.page_lock_count) AS page_lock_min,
       MAX(CTE_2.page_lock_count) AS page_lock_max,
       AVG(CTE_2.page_lock_count) AS page_lock_avg,
       SUM(CTE_2.page_lock_wait_count) AS page_lock_wait,
       MIN(CTE_2.page_lock_wait_count) AS page_lock_wait_min,
       MAX(CTE_2.page_lock_wait_count) AS page_lock_wait_max,
       AVG(CTE_2.page_lock_wait_count) AS page_lock_wait_avg,
       SUM(CTE_2.page_lock_wait_in_ms) AS page_lock_wait_in_ms,
       CONVERT(VARCHAR(200), (SUM(CTE_2.page_lock_wait_in_ms) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(CTE_2.page_lock_wait_in_ms) / 1000), 0), 108) AS page_lock_wait_time_d_h_m_s,
       MIN(CTE_2.page_lock_wait_in_ms) AS page_lock_wait_in_ms_min,
       MAX(CTE_2.page_lock_wait_in_ms) AS page_lock_wait_in_ms_max,
       AVG(CTE_2.page_lock_wait_in_ms) AS page_lock_wait_in_ms_avg,
       CONVERT(NUMERIC(25, 2), 
       CASE 
         WHEN SUM(CTE_2.page_lock_count) > 0 THEN SUM(CTE_2.page_lock_wait_in_ms) / (1. * SUM(CTE_2.page_lock_count))
         ELSE 0 
       END) AS page_lock_avg_per_wait_in_ms,
       /*Lock escalation*/
       SUM(CTE_2.index_lock_promotion_attempt_count) AS index_lock_promotion_attempt,
       MIN(CTE_2.index_lock_promotion_attempt_count) AS index_lock_promotion_attempt_min,
       MAX(CTE_2.index_lock_promotion_attempt_count) AS index_lock_promotion_attempt_max,
       AVG(CTE_2.index_lock_promotion_attempt_count) AS index_lock_promotion_attempt_avg,
       SUM(CTE_2.index_lock_promotion_count) AS index_lock_promotion,
       MIN(CTE_2.index_lock_promotion_count) AS index_lock_promotion_min,
       MAX(CTE_2.index_lock_promotion_count) AS index_lock_promotion_max,
       AVG(CTE_2.index_lock_promotion_count) AS index_lock_promotion_avg,
       /*Forwarded records*/
       SUM(CTE_2.forwarded_fetch_count) AS forwarded_fetch,
       MIN(CTE_2.forwarded_fetch_count) AS forwarded_fetch_min,
       MAX(CTE_2.forwarded_fetch_count) AS forwarded_fetch_max,
       AVG(CTE_2.forwarded_fetch_count) AS forwarded_fetch_avg,
       /*Ghost*/
       SUM(CTE_2.leaf_ghost_count) AS leaf_ghost,
       MIN(CTE_2.leaf_ghost_count) AS leaf_ghost_min,
       MAX(CTE_2.leaf_ghost_count) AS leaf_ghost_max,
       AVG(CTE_2.leaf_ghost_count) AS leaf_ghost_avg,
       /*NonLeaf insert/delete/update/allocation*/
       SUM(CTE_2.nonleaf_insert_count) AS nonleaf_insert,
       MIN(CTE_2.nonleaf_insert_count) AS nonleaf_insert_min,
       MAX(CTE_2.nonleaf_insert_count) AS nonleaf_insert_max,
       AVG(CTE_2.nonleaf_insert_count) AS nonleaf_insert_avg,
       SUM(CTE_2.nonleaf_delete_count) AS nonleaf_delete,
       MIN(CTE_2.nonleaf_delete_count) AS nonleaf_delete_min,
       MAX(CTE_2.nonleaf_delete_count) AS nonleaf_delete_max,
       AVG(CTE_2.nonleaf_delete_count) AS nonleaf_delete_avg,
       SUM(CTE_2.nonleaf_update_count) AS nonleaf_update,
       MIN(CTE_2.nonleaf_update_count) AS nonleaf_update_min,
       MAX(CTE_2.nonleaf_update_count) AS nonleaf_update_max,
       AVG(CTE_2.nonleaf_update_count) AS nonleaf_update_avg,
       SUM(CTE_2.leaf_allocation_count) AS leaf_allocation,
       MIN(CTE_2.leaf_allocation_count) AS leaf_allocation_min,
       MAX(CTE_2.leaf_allocation_count) AS leaf_allocation_max,
       AVG(CTE_2.leaf_allocation_count) AS leaf_allocation_avg,
       SUM(CTE_2.nonleaf_allocation_count) AS nonleaf_allocation,
       MIN(CTE_2.nonleaf_allocation_count) AS nonleaf_allocation_min,
       MAX(CTE_2.nonleaf_allocation_count) AS nonleaf_allocation_max,
       AVG(CTE_2.nonleaf_allocation_count) AS nonleaf_allocation_avg,
       SUM(CTE_2.leaf_page_merge_count) AS leaf_page_merge,
       MIN(CTE_2.leaf_page_merge_count) AS leaf_page_merge_min,
       MAX(CTE_2.leaf_page_merge_count) AS leaf_page_merge_max,
       AVG(CTE_2.leaf_page_merge_count) AS leaf_page_merge_avg,
       SUM(CTE_2.nonleaf_page_merge_count) AS nonleaf_page_merge,
       MIN(CTE_2.nonleaf_page_merge_count) AS nonleaf_page_merge_min,
       MAX(CTE_2.nonleaf_page_merge_count) AS nonleaf_page_merge_max,
       AVG(CTE_2.nonleaf_page_merge_count) AS nonleaf_page_merge_avg,
       /*LOB fetch*/
       SUM(CTE_2.lob_fetch_in_pages) AS lob_fetch_in_pages,
       MIN(CTE_2.lob_fetch_in_pages) AS lob_fetch_in_pages_min,
       MAX(CTE_2.lob_fetch_in_pages) AS lob_fetch_in_pages_max,
       AVG(CTE_2.lob_fetch_in_pages) AS lob_fetch_in_pages_avg,
       SUM(CTE_2.lob_orphan_create_count) AS lob_orphan_create,
       MIN(CTE_2.lob_orphan_create_count) AS lob_orphan_create_min,
       MAX(CTE_2.lob_orphan_create_count) AS lob_orphan_create_max,
       AVG(CTE_2.lob_orphan_create_count) AS lob_orphan_create_avg,
       SUM(CTE_2.lob_orphan_insert_count) AS lob_orphan_insert,
       MIN(CTE_2.lob_orphan_insert_count) AS lob_orphan_insert_min,
       MAX(CTE_2.lob_orphan_insert_count) AS lob_orphan_insert_max,
       AVG(CTE_2.lob_orphan_insert_count) AS lob_orphan_insert_avg,
       /*Row overflow fetch*/
       SUM(CTE_2.row_overflow_fetch_in_pages) AS row_overflow_fetch_in_pages,
       MIN(CTE_2.row_overflow_fetch_in_pages) AS row_overflow_fetch_in_pages_min,
       MAX(CTE_2.row_overflow_fetch_in_pages) AS row_overflow_fetch_in_pages_max,
       AVG(CTE_2.row_overflow_fetch_in_pages) AS row_overflow_fetch_in_pages_avg,
       SUM(CTE_2.row_overflow_fetch_in_bytes) AS row_overflow_fetch_in_bytes,
       MIN(CTE_2.row_overflow_fetch_in_bytes) AS row_overflow_fetch_in_bytes_min,
       MAX(CTE_2.row_overflow_fetch_in_bytes) AS row_overflow_fetch_in_bytes_max,
       AVG(CTE_2.row_overflow_fetch_in_bytes) AS row_overflow_fetch_in_bytes_avg,
       /*Column offrow*/
       SUM(CTE_2.column_value_push_off_row_count) AS column_value_push_off_row,
       MIN(CTE_2.column_value_push_off_row_count) AS column_value_push_off_row_min,
       MAX(CTE_2.column_value_push_off_row_count) AS column_value_push_off_row_max,
       AVG(CTE_2.column_value_push_off_row_count) AS column_value_push_off_row_avg,
       SUM(CTE_2.column_value_pull_in_row_count) AS column_value_pull_in_row,
       MIN(CTE_2.column_value_pull_in_row_count) AS column_value_pull_in_row_min,
       MAX(CTE_2.column_value_pull_in_row_count) AS column_value_pull_in_row_max,
       AVG(CTE_2.column_value_pull_in_row_count) AS column_value_pull_in_row_avg,
       /*Tree page latch and io latch*/
       SUM(CTE_2.tree_page_latch_wait_count) AS tree_page_latch_wait,
       MIN(CTE_2.tree_page_latch_wait_count) AS tree_page_latch_wait_min,
       MAX(CTE_2.tree_page_latch_wait_count) AS tree_page_latch_wait_max,
       AVG(CTE_2.tree_page_latch_wait_count) AS tree_page_latch_wait_avg,
       SUM(CTE_2.tree_page_latch_wait_in_ms) AS tree_page_latch_wait_in_ms,
       MIN(CTE_2.tree_page_latch_wait_in_ms) AS tree_page_latch_wait_in_ms_min,
       MAX(CTE_2.tree_page_latch_wait_in_ms) AS tree_page_latch_wait_in_ms_max,
       AVG(CTE_2.tree_page_latch_wait_in_ms) AS tree_page_latch_wait_in_ms_avg,
       SUM(CTE_2.tree_page_io_latch_wait_count) AS tree_page_io_latch_wait,
       MIN(CTE_2.tree_page_io_latch_wait_count) AS tree_page_io_latch_wait_min,
       MAX(CTE_2.tree_page_io_latch_wait_count) AS tree_page_io_latch_wait_max,
       AVG(CTE_2.tree_page_io_latch_wait_count) AS tree_page_io_latch_wait_avg,
       SUM(CTE_2.tree_page_io_latch_wait_in_ms) AS tree_page_io_latch_wait_in_ms,
       MIN(CTE_2.tree_page_io_latch_wait_in_ms) AS tree_page_io_latch_wait_in_ms_min,
       MAX(CTE_2.tree_page_io_latch_wait_in_ms) AS tree_page_io_latch_wait_in_ms_max,
       AVG(CTE_2.tree_page_io_latch_wait_in_ms) AS tree_page_io_latch_wait_in_ms_avg
INTO tempdb.dbo.tmpIndexCheck43
FROM CTE_2
LEFT OUTER JOIN tempdb.dbo.Tab_GetIndexInfo a
ON CTE_2.database_id = a.Database_ID
AND CTE_2.object_id = a.Object_ID
AND CTE_2.index_id = a.Index_ID
WHERE 1=1
GROUP BY ISNULL(a.Database_Name, CTE_2.database_id),
       ISNULL(a.Schema_Name, '0'),
       ISNULL(a.Table_Name, CTE_2.object_id),
       ISNULL(a.Index_Name, CTE_2.index_id),
       a.Index_Type,
       a.Number_Rows,
       a.last_datetime_obj_was_used,
       a.ReservedSizeInMB,
       a.Buffer_Pool_SpaceUsed_MB,
       a.plan_cache_reference_count,
       a.avg_fragmentation_in_percent,
       a.avg_page_space_used_in_percent

SELECT * FROM tempdb.dbo.tmpIndexCheck43
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name