USE [master];
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_GetIndexInfo')
	EXEC ('CREATE PROC dbo.sp_GetIndexInfo AS SELECT 1')
GO

ALTER PROC dbo.sp_GetIndexInfo
(
  @database_name_filter NVARCHAR(200) = NULL, /* By default I'm collecting information about all DBs */
  @refreshdata  BIT = 0 /* 1 to force drop/create of index tables, 0 will skip table creation if they already exists */
)
/*
-------------------------------------------------------------------------------
| .___            .___              __________            .__                 |
| |   | ____    __| _/____ ___  ___ \______   \ _______  _|__| ______  _  __  |
| |   |/    \  / __ |/ __ \\  \/  /  |       _// __ \  \/ /  |/ __ \ \/ \/ /  |
| |   |   |  \/ /_/ \  ___/ >    <   |    |   \  ___/\   /|  \  ___/\     /   |
| |___|___|  /\____ |\___  >__/\_ \  |____|_  /\___  >\_/ |__|\___  >\/\_/    |
|          \/      \/    \/      \/         \/     \/             \/          |
|                                           __                                |
|                                  |_     |__ |_ . _  _  _    /\  _  _  _. _  |
|                                  |_)\/  |(_||_)|(_|| )(_)  /--\|||(_)| |||| |
-------------------------------------------------------------------------------

sp_GetIndexInfo - March 2023 (v1)

Fabiano Amorim
http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com

For help and more information, visit https://github.com/mcflyamorim/StatisticsReview

How to use:
Collect statistic information for all DBs:
  EXEC sp_GetIndexInfo @database_name_filter = NULL

Collect statistic information for Northwind DB:
  EXEC sp_GetIndexInfo @database_name_filter = 'Northwind', @refreshdata = 1

Credit: 
Some checks and scripts were used based on 
Brent Ozar sp_blitz scripts, MS Tiger team BP, Glenn Berry's diagnostic queries, Kimberly Tripp queries
and probably a lot of other SQL community folks out there, so, a huge kudos for SQL community.

Important notes and pre-requisites:
 * Found a bug or want to change something? Please feel free to create an issue on https://github.com/mcflyamorim/StatisticsReview
   or, you can also e-mail (really? I didn't know people were still using this.) me at fabianonevesamorim@hotmail.com
 * Depending on the number of indexes, the PS script to generate the excel file may use a lot (a few GBs) of memory.

Known issues and limitations:
 * Not tested and not support on Azure SQL DBs, Amazon RDS and Managed Instances (I’m planning to add support for this in a new release).

Disclaimer:
This code and information are provided "AS IS" without warranty of any kind, either expressed or implied.
Furthermore, the author shall not be liable for any damages you may sustain by using this information, whether direct, 
indirect, special, incidental or consequential, even if it has been advised of the possibility of such damages.
	
License:
Pretty much free to everyone and to do anything you'd like as per MIT License - https://en.wikipedia.org/wiki/MIT_License

With all love and care,
Fabiano Amorim

*/
/*
-----------------------------------------------------
-----------------------------------------------------
-----------------------------------------------------
-----------------------------------------------------
TODO - Note to Fabiano.
Change sys.dm_db_index_physical_stats to use sampled for indexes < than 10GB
-----------------------------------------------------
-----------------------------------------------------
-----------------------------------------------------
-----------------------------------------------------
*/

AS
BEGIN

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*1 second*/

DECLARE @statusMsg  VARCHAR(MAX) = ''

/* If data already exists, skip the population, unless refresh was asked via @refreshdata */
IF OBJECT_ID('tempdb.dbo.Tab_GetIndexInfo') IS NOT NULL
BEGIN
  /* 
     I'm assuming data for all tables exists, but I'm only checking tmp_stats... 
     if you're not sure if this is ok, use @refreshdata = 1 to force the refresh and 
     table population
  */
  IF EXISTS(SELECT 1 FROM tempdb.dbo.Tab_GetIndexInfo) AND (@refreshdata = 0)
  BEGIN
			 SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Table with list of indexes already exists, I''ll reuse it and skip the code to populate the table.'
    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
    RETURN
  END
  ELSE
  BEGIN
    DROP TABLE tempdb.dbo.Tab_GetIndexInfo
  END
END

SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Collecting cache index info...'
  RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

IF OBJECT_ID('tempdb.dbo.#tmpCacheMissingIndex1') IS NOT NULL
    DROP TABLE #tmpCacheMissingIndex1;

WITH XMLNAMESPACES
   (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')

SELECT 
       --n.query('.//MissingIndex') AS 'Missing_Index_Cache_Info',
       --CONVERT(XML, n.value('(@StatementText)[1]', 'VARCHAR(4000)')) AS 'Missing_Index_Cache_SQL',
       --n.value('(//MissingIndexGroup/@Impact)[1]', 'FLOAT') AS impact,
       OBJECT_ID(n.value('(//MissingIndex/@Database)[1]', 'VARCHAR(128)') + '.' +
           n.value('(//MissingIndex/@Schema)[1]', 'VARCHAR(128)') + '.' +
           n.value('(//MissingIndex/@Table)[1]', 'VARCHAR(128)')) AS OBJECT_ID
INTO #tmpCacheMissingIndex1
FROM
(
   SELECT query_plan
   FROM (
           SELECT DISTINCT plan_handle
           FROM sys.dm_exec_query_stats WITH(NOLOCK)
         ) AS qs
       OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) tp
   WHERE tp.query_plan.exist('//MissingIndex')=1
) AS tab (query_plan)
CROSS APPLY query_plan.nodes('//StmtSimple') AS q(n)
WHERE n.exist('QueryPlan/MissingIndexes') = 1;

IF OBJECT_ID('tempdb.dbo.#tmpCacheMissingIndex2') IS NOT NULL
    DROP TABLE #tmpCacheMissingIndex2;

SELECT OBJECT_ID, 
       COUNT(*) AS 'Number_of_missing_index_plans_cache'
  INTO #tmpCacheMissingIndex2
  FROM #tmpCacheMissingIndex1
  GROUP BY OBJECT_ID

CREATE CLUSTERED INDEX ix1 ON #tmpCacheMissingIndex2 (OBJECT_ID);

SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Collecting BP usage info...'
RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

IF OBJECT_ID('tempdb.dbo.#tmpBufferDescriptors') IS NOT NULL
    DROP TABLE #tmpBufferDescriptors;

SELECT database_id,
       allocation_unit_id,
       CONVERT(DECIMAL(25, 2), (COUNT(*) * 8) / 1024.) AS CacheSizeMB,
       CONVERT(DECIMAL(25, 2), (SUM(CONVERT(NUMERIC(25,2), free_space_in_bytes)) / 1024.) / 1024.) AS FreeSpaceMB
INTO #tmpBufferDescriptors
FROM sys.dm_os_buffer_descriptors
WHERE dm_os_buffer_descriptors.page_type IN ( 'data_page', 'index_page' )
GROUP BY database_id, allocation_unit_id;

CREATE CLUSTERED INDEX ix1 ON #tmpBufferDescriptors (database_id, allocation_unit_id);

IF OBJECT_ID('tempdb.dbo.Tab_GetIndexInfo') IS NOT NULL
  DROP TABLE tempdb.dbo.Tab_GetIndexInfo

CREATE TABLE tempdb.dbo.Tab_GetIndexInfo
(
  Database_ID INT,
  [Database_Name] [nvarchar] (128) NULL,
  [Schema_Name] [sys].[sysname] NOT NULL,
  [Table_Name] [sys].[sysname] NOT NULL,
  [Index_Name] [sys].[sysname] NULL,
  [Object_ID] INT,
  [Index_ID] INT,
  [Index_Type] [nvarchar] (60) NULL,
  TableHasLOB BIT,
  [Number_Rows] [bigint] NULL,
  [ReservedSizeInMB] [decimal] (18, 2) NULL,
  [reserved_page_count] [bigint] NULL,
  [used_page_count] [bigint] NULL,
  [in_row_data_page_count] [bigint] NULL,
  [Number_Of_Indexes_On_Table] [int] NULL,
  [avg_fragmentation_in_percent] NUMERIC(25,2) NULL,
  [fragment_count] [bigint] NULL,
  [avg_fragment_size_in_pages] NUMERIC(25,2) NULL,
  [page_count] [bigint] NULL,
  [avg_page_space_used_in_percent] NUMERIC(25,2) NULL,
  [record_count] [bigint] NULL,
  [ghost_record_count] [bigint] NULL,
  --[version_ghost_record_count] [bigint] NULL,
  [min_record_size_in_bytes] [int] NULL,
  [max_record_size_in_bytes] [int] NULL,
  [avg_record_size_in_bytes] NUMERIC(25,2) NULL,
  [forwarded_record_count] [bigint] NULL,
  [compressed_page_count] [bigint] NULL,
  --[version_record_count] [bigint] NULL,
  --[inrow_version_record_count] [bigint] NULL,
  --[inrow_diff_version_record_count] [bigint] NULL,
  [fill_factor] [tinyint] NOT NULL,
  [Buffer_Pool_SpaceUsed_MB] [decimal] (18, 2) NOT NULL,
  [Buffer_Pool_FreeSpace_MB] [decimal] (18, 2) NOT NULL,
  [DMV_Missing_Index_Identified] [varchar] (1) NOT NULL,
  [Number_of_missing_index_plans_DMV] [int] NULL,
  [Cache_Missing_Index_Identified] [varchar] (1) NOT NULL,
  [Number_of_missing_index_plans_cache] [int] NULL,
  [Total Writes] [bigint] NULL,
  [Number_of_Reads] [bigint] NULL,
  [Index_was_never_used] [varchar] (1) NOT NULL,
  [indexed_columns] [xml] NULL,
  key_column_name [sys].[sysname],
  key_column_data_type NVARCHAR(250),
  [included_columns] [xml] NULL,
  [is_unique] [bit] NULL,
  [ignore_dup_key] [bit] NULL,
  [is_primary_key] [bit] NULL,
  [is_unique_constraint] [bit] NULL,
  [is_padded] [bit] NULL,
  [is_disabled] [bit] NULL,
  [is_hypothetical] [bit] NULL,
  [allow_row_locks] [bit] NULL,
  [allow_page_locks] [bit] NULL,
  [has_filter] [bit] NULL,
  [filter_definition] [nvarchar] (max) NULL,
  [create_date] [datetime] NOT NULL,
  [modify_date] [datetime] NOT NULL,
  [uses_ansi_nulls] [bit] NULL,
  [is_replicated] [bit] NULL,
  [has_replication_filter] [bit] NULL,
  [text_in_row_limit] [int] NULL,
  [large_value_types_out_of_row] [bit] NULL,
  [is_tracked_by_cdc] [bit] NULL,
  [lock_escalation_desc] [nvarchar] (60) NULL,
  [partition_number] [int] NOT NULL,
  [data_compression_desc] [nvarchar] (60) NULL,
  [user_seeks] [bigint] NULL,
  [user_scans] [bigint] NULL,
  [user_lookups] [bigint] NULL,
  [user_updates] [bigint] NULL,
  [last_user_seek] [datetime] NULL,
  [last_user_scan] [datetime] NULL,
  [last_user_lookup] [datetime] NULL,
  [last_user_update] [datetime] NULL,
  [leaf_insert_count] [bigint] NULL,
  [leaf_delete_count] [bigint] NULL,
  [leaf_update_count] [bigint] NULL,
  [leaf_ghost_count] [bigint] NULL,
  [nonleaf_insert_count] [bigint] NULL,
  [nonleaf_delete_count] [bigint] NULL,
  [nonleaf_update_count] [bigint] NULL,
  [leaf_allocation_count] [bigint] NULL,
  [nonleaf_allocation_count] [bigint] NULL,
  [leaf_page_merge_count] [bigint] NULL,
  [nonleaf_page_merge_count] [bigint] NULL,
  [range_scan_count] [bigint] NULL,
  [singleton_lookup_count] [bigint] NULL,
  [forwarded_fetch_count] [bigint] NULL,
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
  [index_lock_escaltion_attempt_count] [bigint] NULL,
  [index_lock_escaltion_count] [bigint] NULL,
  [page_latch_wait_count] [bigint] NULL,
  [page_latch_wait_in_ms] [bigint] NULL,
  [page_io_latch_wait_count] [bigint] NULL,
  [page_io_latch_wait_in_ms] [bigint] NULL,
  [tree_page_latch_wait_count] [bigint] NULL,
  [tree_page_latch_wait_in_ms] [bigint] NULL,
  [tree_page_io_latch_wait_count] [bigint] NULL,
  [tree_page_io_latch_wait_in_ms] [bigint] NULL,
  [KeyCols_data_length_bytes] INT,
  Key_has_GUID INT,
  IsTablePartitioned BIT,
  last_datetime_obj_was_used DATETIME
)

  DECLARE @sqlmajorver INT
  SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

  /*
    Creating list of DBs we'll collect the information
  */
		SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Creating list of databases to work on.'
  RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

  IF OBJECT_ID('tempdb.dbo.#tmp_db') IS NOT NULL
    DROP TABLE #tmp_db

  CREATE TABLE #tmp_db ([database_name] sysname)

  /* If this is SQL2012+, check AG status */
  IF (@sqlmajorver >= 11 /*SQL2012*/)
  BEGIN    
    BEGIN TRY
      INSERT INTO #tmp_db
      SELECT d1.[name] 
      FROM sys.databases d1
      LEFT JOIN sys.dm_hadr_availability_replica_states hars
      ON d1.replica_id = hars.replica_id
      LEFT JOIN sys.availability_replicas ar
      ON d1.replica_id = ar.replica_id
      WHERE /* Filtering by the specified DB */
      (d1.name = @database_name_filter OR ISNULL(@database_name_filter, '') = '')
      /* I'm not interested to read DBs that are not online :-) */
      AND d1.state_desc = 'ONLINE'
      /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
      AND d1.is_read_only = 0 
      /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
      AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
      /* If DB is part of AG, check only DBs that allow connections */
      AND (  
           (hars.role_desc = 'PRIMARY' OR hars.role_desc IS NULL)
           OR 
           (hars.role_desc = 'SECONDARY' AND ar.secondary_role_allow_connections_desc IN ('READ_ONLY','ALL'))
          )
		  END TRY
		  BEGIN CATCH
			   SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
      SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
		  END CATCH
  END
  /* SQL2008R2 doesn't have AG, so, ignoring the AG DMVs */
  ELSE IF (@sqlmajorver <= 10 /*SQL2008R2*/)
  BEGIN    
    BEGIN TRY
      INSERT INTO #tmp_db
      SELECT d1.[name] 
      FROM sys.databases d1
      WHERE /* Filtering by the specified DB */
      (d1.name = @database_name_filter OR ISNULL(@database_name_filter, '') = '')
      /* I'm not interested to read DBs that are not online :-) */
      AND d1.state_desc = 'ONLINE'
      /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
      AND d1.is_read_only = 0 
      /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
      AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
		  END TRY
		  BEGIN CATCH
			   SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
      SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
		  END CATCH
  END

DECLARE @SQL VarCHar(MAX)
declare @database_name sysname

DECLARE c_databases CURSOR read_only FOR
    SELECT [database_name] FROM #tmp_db
OPEN c_databases

FETCH NEXT FROM c_databases
into @database_name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Working on DB - [' + @database_name + ']'
  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT

  SET @SQL = 'use [' + @database_name + ']; ' + 

  'DECLARE @statusMsg  VARCHAR(MAX) = ''''
  
  SET NOCOUNT ON;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET LOCK_TIMEOUT 1000; /*1 second*/

  IF OBJECT_ID(''tempdb.dbo.#tmp_dm_db_index_usage_stats'') IS NOT NULL
    DROP TABLE #tmp_dm_db_index_usage_stats
  BEGIN TRY
    /* Creating a copy of sys.dm_db_index_usage_stats because this is too slow to access without an index */
    SELECT database_id, object_id, index_id, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, last_user_update
      INTO #tmp_dm_db_index_usage_stats 
      FROM sys.dm_db_index_usage_stats AS ius WITH(NOLOCK)
      WHERE ius.database_id = DB_ID()
  END TRY
  BEGIN CATCH
    SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error while trying to read data from sys.dm_db_index_usage_stats. You may see limited results because of it.''
    RAISERROR (@statusMsg, 0,0) WITH NOWAIT
  END CATCH

  CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_usage_stats (database_id, object_id, index_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_dm_db_index_operational_stats'') IS NOT NULL
    DROP TABLE #tmp_dm_db_index_operational_stats

  BEGIN TRY
    /* Creating a copy of sys.dm_db_index_operational_stats because this is too slow to access without an index */
    /* Aggregating the results, to have total for all partitions */
    SELECT DB_ID() AS database_id,
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
           CONVERT(VARCHAR(10), (SUM(page_latch_wait_in_ms) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(page_latch_wait_in_ms) / 1000), 0), 108) AS page_latch_wait_time_d_h_m_s,
           SUM(page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms,
           CONVERT(NUMERIC(25, 2), 
           CASE 
             WHEN SUM(page_io_latch_wait_count) > 0 THEN SUM(page_io_latch_wait_in_ms) / (1. * SUM(page_io_latch_wait_count))
             ELSE 0 
           END) AS avg_page_io_latch_wait_in_ms,
           CONVERT(VARCHAR(10), (SUM(page_io_latch_wait_in_ms) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(page_io_latch_wait_in_ms) / 1000), 0), 108) AS page_io_latch_wait_time_d_h_m_s
      INTO #tmp_dm_db_index_operational_stats
      FROM sys.dm_db_index_operational_stats (DB_ID(), NULL, NULL, NULL) AS ios
     GROUP BY object_id, 
           index_id
  END TRY
  BEGIN CATCH
    SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error while trying to read data from sys.dm_db_index_operational_stats. You may see limited results because of it.''
    RAISERROR (@statusMsg, 0,0) WITH NOWAIT
  END CATCH

  CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_operational_stats (database_id, object_id, index_id)

  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Collecting fragmentation index info...''
  RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

  SET LOCK_TIMEOUT 5000; /*5 seconds*/
  DECLARE @objname sysname, @idxname sysname, @object_id INT, @index_id INT, @row_count VARCHAR(50), @tot INT, @i INT

  IF OBJECT_ID(''tempdb.dbo.#tmpIndexFrag'') IS NOT NULL
    DROP TABLE #tmpIndexFrag;

  CREATE TABLE [#tmpIndexFrag]
  (
    [database_id] SMALLINT,
    [object_id] INT,
    [index_id] INT,
    [avg_fragmentation_in_percent] NUMERIC(25,2),
    [fragment_count] BIGINT,
    [avg_fragment_size_in_pages] NUMERIC(25,2),
    [page_count] BIGINT,
    [avg_page_space_used_in_percent] NUMERIC(25,2),
    [record_count] BIGINT,
    [ghost_record_count] BIGINT,
    [min_record_size_in_bytes] INT,
    [max_record_size_in_bytes] INT,
    [avg_record_size_in_bytes] NUMERIC(25,2),
    [forwarded_record_count] BIGINT,
    [compressed_page_count] BIGINT
  )

  IF OBJECT_ID(''tempdb.dbo.#tmpIndexFrag_Cursor'') IS NOT NULL
    DROP TABLE #tmpIndexFrag_Cursor;

  SELECT objects.name AS objname, ISNULL(indexes.name, ''HEAP'') AS idxname, indexes.object_id, indexes.index_id, PARSENAME(CONVERT(VARCHAR(50), CONVERT(MONEY, dm_db_partition_stats.row_count), 1), 2) AS row_count
  INTO #tmpIndexFrag_Cursor
  FROM sys.indexes
  INNER JOIN sys.objects
  ON objects.object_id = indexes.object_id
  INNER JOIN sys.dm_db_partition_stats
  ON dm_db_partition_stats.object_id = indexes.object_id
  AND dm_db_partition_stats.index_id = indexes.index_id
  WHERE objects.type = ''U''
  AND indexes.type not in (5, 6) /*ignoring columnstore indexes*/
  AND dm_db_partition_stats.partition_number = 1

  SET @tot = @@ROWCOUNT

  DECLARE c_allrows CURSOR READ_ONLY FOR
  SELECT * FROM #tmpIndexFrag_Cursor
  ORDER BY row_count ASC

  OPEN c_allrows

  FETCH NEXT FROM c_allrows
  INTO @objname, @idxname, @object_id, @index_id, @row_count

  SET @i = 0
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @i = @i + 1

    SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Working on index '' + CONVERT(VARCHAR, @i) + '' of '' + CONVERT(VARCHAR, @tot) + '': ObjName = '' + QUOTENAME(@objname) + '' | IndexName = '' + QUOTENAME(@idxname) + '' | RowCount = '' + @row_count
    RAISERROR (@statusMsg, 10, 1) WITH NOWAIT

    BEGIN TRY
      INSERT INTO #tmpIndexFrag
      SELECT
        dm_db_index_physical_stats.database_id,
        dm_db_index_physical_stats.object_id,
        dm_db_index_physical_stats.index_id,
        dm_db_index_physical_stats.avg_fragmentation_in_percent,
        dm_db_index_physical_stats.fragment_count,
        dm_db_index_physical_stats.avg_fragment_size_in_pages,
        dm_db_index_physical_stats.page_count,
        dm_db_index_physical_stats.avg_page_space_used_in_percent,
        dm_db_index_physical_stats.record_count,
        dm_db_index_physical_stats.ghost_record_count,
        dm_db_index_physical_stats.min_record_size_in_bytes,
        dm_db_index_physical_stats.max_record_size_in_bytes,
        dm_db_index_physical_stats.avg_record_size_in_bytes,
        dm_db_index_physical_stats.forwarded_record_count,
        dm_db_index_physical_stats.compressed_page_count
      FROM sys.dm_db_index_physical_stats(DB_ID(), @object_id, @index_id, NULL, CASE WHEN LEN(@row_count) >= 11 THEN ''LIMITED'' ELSE ''SAMPLED'' END)
      WHERE dm_db_index_physical_stats.alloc_unit_type_desc = ''IN_ROW_DATA''
      AND index_level = 0 /*leaf-level nodes only*/
      AND partition_number = 1
      OPTION (RECOMPILE);
    END TRY
    BEGIN CATCH
      SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error trying to run fragmentation index query... Timeout... Skipping collection data about this table/index.''
      RAISERROR (@statusMsg, 10, 1) WITH NOWAIT
    END CATCH

    FETCH NEXT FROM c_allrows
    INTO @objname, @idxname, @object_id, @index_id, @row_count
  END
  CLOSE c_allrows
  DEALLOCATE c_allrows

  /* Creating a copy of sys.partitions and sys.allocation_units because unindexed access to it can be very slow */
  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_partitions'') IS NOT NULL
      DROP TABLE #tmp_sys_partitions;
  SELECT * INTO #tmp_sys_partitions FROM sys.partitions
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_partitions (object_id, index_id, partition_number)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_allocation_units'') IS NOT NULL
      DROP TABLE #tmp_sys_allocation_units;
  SELECT * INTO #tmp_sys_allocation_units FROM sys.allocation_units
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_allocation_units (container_id)

  SELECT DB_ID() AS database_id,
         DB_NAME() AS ''Database_Name'',
         sc.name AS ''Schema_Name'',
         t.name AS ''Table_Name'',
         i.name AS ''Index_Name'',
         t.object_id,
         i.index_id,
         i.type_desc AS ''Index_Type'',
         ISNULL(t1.TableHasLOB, 0) AS TableHasLOB,
         p.rows AS ''Number_Rows'',
         tSize.ReservedSizeInMB,
         tSize.reserved_page_count,
         tSize.used_page_count,
         tSize.in_row_data_page_count,
         tNumerOfIndexes.Cnt AS ''Number_Of_Indexes_On_Table'',
         #tmpIndexFrag.avg_fragmentation_in_percent,
         #tmpIndexFrag.fragment_count,
         #tmpIndexFrag.avg_fragment_size_in_pages,
         #tmpIndexFrag.page_count,
         #tmpIndexFrag.avg_page_space_used_in_percent,
         #tmpIndexFrag.record_count,
         #tmpIndexFrag.ghost_record_count,
         --#tmpIndexFrag.version_ghost_record_count,
         #tmpIndexFrag.min_record_size_in_bytes,
         #tmpIndexFrag.max_record_size_in_bytes,
         #tmpIndexFrag.avg_record_size_in_bytes,
         #tmpIndexFrag.forwarded_record_count,
         #tmpIndexFrag.compressed_page_count,
         --#tmpIndexFrag.version_record_count,
         --#tmpIndexFrag.inrow_version_record_count,
         --#tmpIndexFrag.inrow_diff_version_record_count,
         i.fill_factor,
         ISNULL(bp.CacheSizeMB, 0) AS ''Buffer_Pool_SpaceUsed_MB'',
         ISNULL(bp.FreeSpaceMB, 0) AS ''Buffer_Pool_FreeSpace_MB'',
         CASE
             WHEN mid.database_id IS NULL THEN
                 ''N''
             ELSE
                 ''Y''
         END AS ''DMV_Missing_Index_Identified'',
         mid.Number_of_missing_index_plans_DMV,
         CASE
             WHEN #tmpCacheMissingIndex2.Number_of_missing_index_plans_cache IS NULL THEN
                 ''N''
             ELSE
                 ''Y''
         END AS ''Cache_Missing_Index_Identified'',
         #tmpCacheMissingIndex2.Number_of_missing_index_plans_cache,
         ius.user_updates AS [Total Writes], 
         ius.user_seeks + ius.user_scans + ius.user_lookups AS ''Number_of_Reads'',
         CASE
             WHEN ius.user_seeks + ius.user_scans + ius.user_lookups = 0 THEN
                 ''Y''
             ELSE
                 ''N''
         END AS ''Index_was_never_used'',
         CONVERT(XML, ISNULL(REPLACE(REPLACE(REPLACE(
                                (
                                    SELECT c.name AS ''columnName''
                                    FROM sys.index_columns AS sic
                                        JOIN sys.columns AS c
                                            ON c.column_id = sic.column_id
                                               AND c.object_id = sic.object_id
                                    WHERE sic.object_id = i.object_id
                                          AND sic.index_id = i.index_id
                                          AND is_included_column = 0
                                    ORDER BY sic.index_column_id
                                    FOR XML RAW
                                ),
                                ''"/><row columnName="'',
                                '', ''
                                       ),
                                ''<row columnName="'',
                                ''''
                               ),
                        ''"/>'',
                        ''''
                       ),
                ''''
               )) AS ''indexed_columns'',
         ISNULL(tab_index_key_column.indexkeycolumnname, '''') AS key_column_name,
         ISNULL(tab_index_key_column.keycolumndatatype, '''') AS key_column_data_type,
         CONVERT(XML, ISNULL(REPLACE(REPLACE(REPLACE(
                                (
                                    SELECT c.name AS ''columnName''
                                    FROM sys.index_columns AS sic
                                        JOIN sys.columns AS c
                                            ON c.column_id = sic.column_id
                                               AND c.object_id = sic.object_id
                                    WHERE sic.object_id = i.object_id
                                          AND sic.index_id = i.index_id
                                          AND is_included_column = 1
                                    ORDER BY sic.index_column_id
                                    FOR XML RAW
                                ),
                                ''"/><row columnName="'',
                                '', ''
                                       ),
                                ''<row columnName="'',
                                ''''
                               ),
                        ''"/>'',
                        ''''
                       ),
                ''''
               )) AS ''included_columns'',
         i.is_unique,
         i.ignore_dup_key,
         i.is_primary_key,
         i.is_unique_constraint,
         i.is_padded,
         i.is_disabled,
         i.is_hypothetical,
         i.allow_row_locks,
         i.allow_page_locks,
         i.has_filter,
         i.filter_definition,
         t.create_date,
         t.modify_date,
         t.uses_ansi_nulls,
         t.is_replicated,
         t.has_replication_filter,
         t.text_in_row_limit,
         t.large_value_types_out_of_row,
         t.is_tracked_by_cdc,
         t.lock_escalation_desc,
         --t.is_filetable,
         --t.is_memory_optimized,
         --t.durability_desc,
         --t.temporal_type_desc,
         --t.is_remote_data_archive_enabled,
         p.partition_number,
         p.data_compression_desc,
         ius.user_seeks,
         ius.user_scans,
         ius.user_lookups,
         ius.user_updates,
         ius.last_user_seek,
         ius.last_user_scan,
         ius.last_user_lookup,
         ius.last_user_update,
         ios.leaf_insert_count,
         ios.leaf_delete_count,
         ios.leaf_update_count,
         ios.leaf_ghost_count,
         ios.nonleaf_insert_count,
         ios.nonleaf_delete_count,
         ios.nonleaf_update_count,
         ios.leaf_allocation_count,
         ios.nonleaf_allocation_count,
         ios.leaf_page_merge_count,
         ios.nonleaf_page_merge_count,
         ios.range_scan_count,
         ios.singleton_lookup_count,
         ios.forwarded_fetch_count,
         ios.lob_fetch_in_pages,
         ios.lob_fetch_in_bytes,
         ios.lob_orphan_create_count,
         ios.lob_orphan_insert_count,
         ios.row_overflow_fetch_in_pages,
         ios.row_overflow_fetch_in_bytes,
         ios.column_value_push_off_row_count,
         ios.column_value_pull_in_row_count,
         ios.row_lock_count,
         ios.row_lock_wait_count,
         ios.row_lock_wait_in_ms,
         ios.page_lock_count,
         ios.page_lock_wait_count,
         ios.page_lock_wait_in_ms,
         ios.index_lock_promotion_attempt_count AS index_lock_escaltion_attempt_count,
         ios.index_lock_promotion_count AS index_lock_escaltion_count,
         ios.page_latch_wait_count,
         ios.page_latch_wait_in_ms,
         ios.page_io_latch_wait_count,
         ios.page_io_latch_wait_in_ms,
         ios.tree_page_latch_wait_count,
         ios.tree_page_latch_wait_in_ms,
         ios.tree_page_io_latch_wait_count,
         ios.tree_page_io_latch_wait_in_ms,
         (SELECT SUM(CASE sty.name WHEN ''nvarchar'' THEN sc.max_length/2 ELSE sc.max_length END) 
          FROM sys.indexes AS ii
		        INNER JOIN sys.tables AS tt ON tt.[object_id] = ii.[object_id]
		        INNER JOIN sys.schemas ss ON ss.[schema_id] = tt.[schema_id]
		        INNER JOIN sys.index_columns AS sic ON sic.object_id = tt.object_id AND sic.index_id = ii.index_id
		        INNER JOIN sys.columns AS sc ON sc.object_id = tt.object_id AND sc.column_id = sic.column_id
		        INNER JOIN sys.types AS sty ON sc.user_type_id = sty.user_type_id
		        WHERE ii.[object_id] = i.[object_id] 
            AND ii.index_id = i.index_id 
            AND sic.key_ordinal > 0) AS [KeyCols_data_length_bytes],
         (SELECT COUNT(sty.name) 
            FROM sys.indexes AS ii
		         INNER JOIN sys.tables AS tt ON tt.[object_id] = ii.[object_id]
		         INNER JOIN sys.schemas ss ON ss.[schema_id] = tt.[schema_id]
		         INNER JOIN sys.index_columns AS sic ON sic.object_id = i.object_id AND sic.index_id = i.index_id
		         INNER JOIN sys.columns AS sc ON sc.object_id = tt.object_id AND sc.column_id = sic.column_id
		         INNER JOIN sys.types AS sty ON sc.user_type_id = sty.user_type_id
		         WHERE i.[object_id] = ii.[object_id] 
           AND i.index_id = ii.index_id 
           AND sic.is_included_column = 0 
           AND sty.name = ''uniqueidentifier'') AS [Key_has_GUID],
         CASE 
           WHEN EXISTS(SELECT *
                         FROM #tmp_sys_partitions pp
                        WHERE pp.partition_number > 1
                          AND pp.object_id = i.object_Id
                          AND pp.index_id IN (0, 1)) THEN 1
           ELSE 0
         END AS IsTablePartitioned,
         TabIndexUsage.last_datetime_obj_was_used
  FROM sys.indexes i WITH (NOLOCK)
      INNER JOIN sys.tables t
          ON t.object_id = i.object_id
      INNER JOIN sys.schemas sc WITH (NOLOCK)
          ON sc.schema_id = t.schema_id
      INNER JOIN #tmp_sys_partitions AS p
          ON i.object_id = p.object_id
             AND i.index_id = p.index_id
             AND p.partition_number = 1
      INNER JOIN #tmp_sys_allocation_units AS au
          ON au.container_id = p.hobt_id
         AND au.type_desc = ''IN_ROW_DATA''
      LEFT OUTER JOIN #tmpCacheMissingIndex2
        ON #tmpCacheMissingIndex2.OBJECT_ID = i.object_id
      OUTER APPLY (SELECT TOP 1 
                          1 AS TableHasLOB
                  FROM sys.tables
                 INNER JOIN sys.all_columns
                    ON all_columns.object_id = tables.object_id
                  WHERE i.Object_ID = tables.object_id
                    AND COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = -1) as t1
      CROSS APPLY
      (
          SELECT CONVERT(DECIMAL(18, 2), SUM((st.reserved_page_count * 8) / 1024.)) ReservedSizeInMB,
                 SUM(st.reserved_page_count) AS reserved_page_count,
                 SUM(st.used_page_count) AS used_page_count,
                 SUM(st.in_row_data_page_count) AS in_row_data_page_count
          FROM sys.dm_db_partition_stats st
          WHERE i.object_id = st.object_id
                AND i.index_id = st.index_id
                AND p.partition_number = st.partition_number
                AND st.partition_number = 1
      ) AS tSize
      LEFT OUTER JOIN #tmp_dm_db_index_usage_stats ius WITH (NOLOCK)
          ON ius.index_id = i.index_id
             AND ius.object_id = i.object_id
             AND ius.database_id = DB_ID()
      LEFT OUTER JOIN #tmp_dm_db_index_operational_stats AS ios WITH (NOLOCK)
          ON ios.database_id = DB_ID()
         AND ios.object_id = i.object_id
         AND ios.index_id = i.index_id
      LEFT OUTER JOIN #tmpBufferDescriptors AS bp
          ON bp.database_id = DB_ID()
         AND bp.allocation_unit_id = au.allocation_unit_id
      LEFT OUTER JOIN #tmpIndexFrag
          ON i.object_id = #tmpIndexFrag.object_id
             AND i.index_id = #tmpIndexFrag.index_id
             AND #tmpIndexFrag.database_id = DB_ID()
      LEFT OUTER JOIN
      (
          SELECT database_id,
                 object_id,
                 COUNT(*) AS Number_of_missing_index_plans_DMV
          FROM sys.dm_db_missing_index_details
          GROUP BY database_id,
                   object_id
      ) AS mid
          ON mid.database_id = DB_ID()
             AND mid.object_id = i.object_id
      CROSS APPLY
      (
          SELECT COUNT(*) AS Cnt
          FROM sys.indexes i1
          WHERE i.object_id = i1.object_id
      ) AS tNumerOfIndexes
      OUTER APPLY (SELECT all_columns.Name AS indexkeycolumnname, 
                           CASE 
                             WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = -1 THEN 1
                             WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = 2147483647 THEN 1
                             ELSE 0
                           END AS islob,
                           UPPER(TYPE_NAME(types.system_type_id)) + '' (precision = '' + 
                           CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'')) + 
                           '', scale = '' +
                           ISNULL(CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Scale'')), ''0'') + 
                           '')'' AS keycolumndatatype
                   FROM sys.index_columns
                   INNER JOIN sys.all_columns
                   ON all_columns.object_id = index_columns.object_id
                   AND all_columns.column_id = index_columns.column_id
                   INNER JOIN sys.types
                   ON types.user_type_id = all_columns.user_type_id
                   WHERE i.object_id = index_columns.object_id
                   AND i.index_id = index_columns.index_id
                   AND index_columns.key_ordinal = 1
                   AND index_columns.is_included_column = 0) AS tab_index_key_column
       OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(ius.last_user_seek), 
                                               (ius.last_user_scan),
                                               (ius.last_user_lookup)
                                      ) AS t(Dt)) AS TabIndexUsage(last_datetime_obj_was_used)
  WHERE OBJECTPROPERTY(i.[object_id], ''IsUserTable'') = 1
  ORDER BY tSize.ReservedSizeInMB DESC
  '

  /*
    SELECT @SQL
  */  
  
  INSERT INTO tempdb.dbo.Tab_GetIndexInfo
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @database_name
END
CLOSE c_databases
DEALLOCATE c_databases

CREATE UNIQUE CLUSTERED INDEX ix1 ON tempdb.dbo.Tab_GetIndexInfo(Database_ID, Object_ID, Index_ID)
CREATE INDEX ix2 ON tempdb.dbo.Tab_GetIndexInfo(Database_Name, Schema_Name, Table_Name) INCLUDE(Index_ID, Number_Rows)

--SELECT * FROM tempdb.dbo.Tab_GetIndexInfo
--ORDER BY ReservedSizeInMB DESC
END
GO
