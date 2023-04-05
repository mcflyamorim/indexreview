/* 
  Check 7 - Xtp Too Many Buckets
*/

--------------------------------------------------------------------------------------------------------------------------------
-- Index Health Analysis subsection
--------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

DECLARE @sqlcmd NVARCHAR(MAX),
        @params NVARCHAR(600),
        @sqlmajorver INT;
DECLARE @dbid INT,
        @dbname NVARCHAR(1000);
DECLARE @ErrorSeverity INT,
        @ErrorState INT,
        @ErrorMessage NVARCHAR(4000);


SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff);

--RAISERROR(N'  |-Starting Index Health Analysis check', 10, 1) WITH NOWAIT;
DECLARE /*@dbid int, */ @objectid INT,
                        @indexid INT,
                        @partition_nr INT,
                        @type_desc NVARCHAR(60);
DECLARE @ColumnStoreGetIXSQL NVARCHAR(2000),
        @ColumnStoreGetIXSQL_Param NVARCHAR(1000),
        @HasInMem BIT;
DECLARE /*@sqlcmd NVARCHAR(4000), @params NVARCHAR(500),*/ @schema_name VARCHAR(100),
                                                           @table_name VARCHAR(300),
                                                           @KeyCols VARCHAR(4000),
                                                           @distinctCnt BIGINT,
                                                           @OptimBucketCnt BIGINT;


IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpIPS_CI')
)
    DROP TABLE #tmpIPS_CI;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpIPS_CI')
)
    CREATE TABLE #tmpIPS_CI
    (
        [database_id] INT,
        [object_id] INT,
        [index_id] INT,
        [partition_number] INT,
        fragmentation DECIMAL(18, 3),
        [page_count] BIGINT,
        [size_MB] DECIMAL(26, 3),
        record_count BIGINT,
        delta_store_hobt_id BIGINT,
        row_group_id INT,
        [state] TINYINT,
        state_description VARCHAR(60),
        CONSTRAINT PK_IPS_CI_Check7
            PRIMARY KEY CLUSTERED (
                                      database_id,
                                      [object_id],
                                      [index_id],
                                      [partition_number],
                                      row_group_id
                                  )
    );

IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpXIS')
)
    DROP TABLE #tmpXIS;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpXIS')
)
    CREATE TABLE #tmpXIS
    (
        [database_id] INT,
        [object_id] INT,
        [xtp_object_id] INT,
        [schema_name] VARCHAR(100) COLLATE DATABASE_DEFAULT,
        [table_name] VARCHAR(300) COLLATE DATABASE_DEFAULT,
        [index_id] INT,
        [index_name] VARCHAR(300) COLLATE DATABASE_DEFAULT,
        type_desc NVARCHAR(60),
        total_bucket_count BIGINT,
        empty_bucket_count BIGINT,
        avg_chain_length BIGINT,
        max_chain_length BIGINT,
        KeyCols VARCHAR(4000) COLLATE DATABASE_DEFAULT,
        DistinctCnt BIGINT NULL,
        OptimBucketCnt BIGINT NULL,
        isdone BIT,
        CONSTRAINT PK_tmpXIS_Check7
            PRIMARY KEY CLUSTERED (
                                      database_id,
                                      [object_id],
                                      [xtp_object_id],
                                      [index_id]
                                  )
    );

IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpXNCIS')
)
    DROP TABLE #tmpXNCIS;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpXNCIS')
)
    CREATE TABLE #tmpXNCIS
    (
        [database_id] INT,
        [object_id] INT,
        [xtp_object_id] INT,
        [schema_name] VARCHAR(100) COLLATE DATABASE_DEFAULT,
        [table_name] VARCHAR(300) COLLATE DATABASE_DEFAULT,
        [index_id] INT,
        [index_name] VARCHAR(300) COLLATE DATABASE_DEFAULT,
        type_desc NVARCHAR(60),
        delta_pages BIGINT,
        internal_pages BIGINT,
        leaf_pages BIGINT,
        page_update_count BIGINT,
        page_update_retry_count BIGINT,
        page_consolidation_count BIGINT,
        page_consolidation_retry_count BIGINT,
        page_split_count BIGINT,
        page_split_retry_count BIGINT,
        key_split_count BIGINT,
        key_split_retry_count BIGINT,
        page_merge_count BIGINT,
        page_merge_retry_count BIGINT,
        key_merge_count BIGINT,
        key_merge_retry_count BIGINT,
        scans_started BIGINT,
        scans_retries BIGINT,
        CONSTRAINT PK_tmpXNCIS_Check7
            PRIMARY KEY CLUSTERED (
                                      database_id,
                                      [object_id],
                                      [xtp_object_id],
                                      [index_id]
                                  )
    );

IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblWorking')
)
    DROP TABLE #tblWorking;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblWorking')
)
    CREATE TABLE #tblWorking
    (
        database_id INT,
        [database_name] NVARCHAR(255),
        [object_id] INT,
        [object_name] NVARCHAR(255),
        index_id INT,
        index_name NVARCHAR(255),
        [schema_name] NVARCHAR(255),
        partition_number INT,
        [type] TINYINT,
        type_desc NVARCHAR(60),
        is_done BIT
    );
-- type 0 = Heap; 1 = Clustered; 2 = Nonclustered; 3 = XML; 4 = Spatial; 5 = Clustered columnstore; 6 = Nonclustered columnstore; 7 = Nonclustered hash

--RAISERROR(N'    |-Populating support table...', 10, 1) WITH NOWAIT;

IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0')
)
    DROP TABLE #tmpdbs0;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0')
)
    CREATE TABLE #tmpdbs0
    (
        id INT IDENTITY(1, 1),
        [dbid] INT,
        [dbname] NVARCHAR(1000),
        is_read_only BIT,
        [state] TINYINT,
        isdone BIT
    );

SET @sqlcmd
    = N'SELECT database_id, name, is_read_only, [state], 0 FROM master.sys.databases (NOLOCK) 
                 WHERE name in (select Database_name FROM tempdb.dbo.Tab_GetIndexInfo)';
INSERT INTO #tmpdbs0
(
    [dbid],
    [dbname],
    is_read_only,
    [state],
    [isdone]
)
EXEC sp_executesql @sqlcmd;

IF EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblHypObj')
)
    DROP TABLE #tblHypObj;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblHypObj')
)
    CREATE TABLE #tblHypObj
    (
        [DBName] sysname,
        [Schema] VARCHAR(100),
        [Table] VARCHAR(255),
        [Object] VARCHAR(255),
        [Type] VARCHAR(10)
    );

UPDATE #tmpdbs0
SET isdone = 0;

UPDATE #tmpdbs0
SET isdone = 1
WHERE [state] <> 0
      OR [dbid] = 2;

IF EXISTS (SELECT TOP 1 id FROM #tmpdbs0 WHERE isdone = 0)
BEGIN
    WHILE
    (SELECT COUNT(id)FROM #tmpdbs0 WHERE isdone = 0) > 0
    BEGIN
        SELECT TOP 1
               @dbname = [dbname],
               @dbid = [dbid]
        FROM #tmpdbs0
        WHERE isdone = 0;

        IF
        (
            SELECT CHARINDEX(CHAR(39), @dbname)
        ) > 0
        OR
        (
            SELECT CHARINDEX(CHAR(45), @dbname)
        ) > 0
        OR
        (
            SELECT CHARINDEX(CHAR(47), @dbname)
        ) > 0
        BEGIN
            SELECT @ErrorMessage
                = N'    |-Skipping Database ID ' + CONVERT(VARCHAR, DB_ID(QUOTENAME(@dbname)))
                  + N' due to potential of SQL Injection';
            RAISERROR(@ErrorMessage, 10, 1) WITH NOWAIT;
        END;
        ELSE
        BEGIN
            SELECT @sqlcmd
                = N'SELECT ' + CONVERT(VARCHAR(10), @dbid) + N', ''' + DB_NAME(@dbid)
                  + N''', si.[object_id], mst.[name], si.index_id, si.name, t.name, sp.partition_number, si.[type], si.type_desc, 0
                  FROM ['    + @dbname + N'].sys.indexes si
                  INNER JOIN [' + @dbname
                  + N'].sys.partitions sp ON si.[object_id] = sp.[object_id] AND si.index_id = sp.index_id
                  INNER JOIN [' + @dbname
                  + N'].sys.tables AS mst ON mst.[object_id] = si.[object_id]
                  INNER JOIN [' + @dbname
                  + N'].sys.schemas AS t ON t.[schema_id] = mst.[schema_id]
                  WHERE mst.is_ms_shipped = 0 AND ' + CASE
                                                          WHEN @sqlmajorver <= 11 THEN
                                                              ' si.[type] <= 2;'
                                                          ELSE
                                                              ' si.[type] IN (0,1,2,5,6,7);'
                                                      END;

            INSERT INTO #tblWorking
            EXEC sp_executesql @sqlcmd;

            IF @sqlmajorver >= 12
            BEGIN
                SELECT @sqlcmd
                    = N'SELECT @HasInMemOUT = ISNULL((SELECT TOP 1 1 FROM [' + @dbname
                      + N'].sys.filegroups FG where FG.[type] = ''FX''), 0)';
                SET @params = N'@HasInMemOUT bit OUTPUT';
                EXECUTE sp_executesql @sqlcmd, @params, @HasInMemOUT = @HasInMem OUTPUT;

                IF @HasInMem = 1
                BEGIN
                    INSERT INTO #tmpIPS_CI
                    (
                        [database_id],
                        [object_id],
                        [index_id],
                        [partition_number],
                        fragmentation,
                        [page_count],
                        [size_MB],
                        record_count,
                        delta_store_hobt_id,
                        row_group_id,
                        [state],
                        state_description
                    )
                    EXECUTE sp_executesql @ColumnStoreGetIXSQL,
                                          @ColumnStoreGetIXSQL_Param,
                                          @dbid_In = @dbid,
                                          @objectid_In = @objectid,
                                          @indexid_In = @indexid,
                                          @partition_nr_In = @partition_nr;

                    SELECT @ErrorMessage
                        = N'    |-Gathering sys.dm_db_xtp_hash_index_stats and sys.dm_db_xtp_nonclustered_index_stats data in '
                          + @dbname + N'...';
                    RAISERROR(@ErrorMessage, 10, 1) WITH NOWAIT;

                    SET @sqlcmd
                        = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                          USE [' + @dbname + N'];
                          SELECT ' + CONVERT(NVARCHAR(20), @dbid)
                          + N' AS [database_id], xis.[object_id], xhis.xtp_object_id, t.name, o.name, xis.index_id, si.name, si.type_desc, xhis.total_bucket_count, xhis.empty_bucket_count, xhis.avg_chain_length, xhis.max_chain_length,
	                          SUBSTRING((SELECT '','' + ac.name FROM sys.tables AS st
		                          INNER JOIN sys.indexes AS i ON st.[object_id] = i.[object_id]
		                          INNER JOIN sys.index_columns AS ic ON i.[object_id] = ic.[object_id] AND i.[index_id] = ic.[index_id] 
		                          INNER JOIN sys.all_columns AS ac ON st.[object_id] = ac.[object_id] AND ic.[column_id] = ac.[column_id]
		                          WHERE si.[object_id] = i.[object_id] AND si.index_id = i.index_id AND ic.is_included_column = 0
		                          ORDER BY ic.key_ordinal
	                          FOR XML PATH('''')), 2, 8000) AS KeyCols, NULL, NULL, 0
                          FROM sys.dm_db_xtp_hash_index_stats AS xhis
                          INNER JOIN sys.dm_db_xtp_index_stats AS xis ON xis.[object_id] = xhis.[object_id] AND xis.[index_id] = xhis.[index_id] 
                          INNER JOIN sys.indexes AS si (NOLOCK) ON xis.[object_id] = si.[object_id] AND xis.[index_id] = si.[index_id]
                          INNER JOIN sys.objects AS o (NOLOCK) ON si.[object_id] = o.[object_id]
                          INNER JOIN sys.tables AS mst (NOLOCK) ON mst.[object_id] = o.[object_id]
                          INNER JOIN sys.schemas AS t (NOLOCK) ON t.[schema_id] = mst.[schema_id]
                          WHERE o.[type] = ''U''';

                    BEGIN TRY
                        INSERT INTO #tmpXIS
                        EXECUTE sp_executesql @sqlcmd;
                    END TRY
                    BEGIN CATCH
                        SET @ErrorMessage
                            = N'      |-Error ' + CONVERT(VARCHAR(20), ERROR_NUMBER())
                              + N' has occurred while analyzing hash indexes. Message: ' + ERROR_MESSAGE()
                              + N' (Line Number: ' + CAST(ERROR_LINE() AS VARCHAR(10)) + N')';
                        RAISERROR(@ErrorMessage, 0, 42) WITH NOWAIT;
                    END CATCH;

                    SET @sqlcmd
                        = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                          USE [' + @dbname + N'];
                          SELECT DISTINCT ' + CONVERT(NVARCHAR(20), @dbid)
                          + N' AS [database_id],
	                          xis.[object_id], xnis.xtp_object_id, t.name, o.name, xis.index_id, si.name, si.type_desc,
	                          xnis.delta_pages, xnis.internal_pages, xnis.leaf_pages, xnis.page_update_count,
	                          xnis.page_update_retry_count, xnis.page_consolidation_count,
	                          xnis.page_consolidation_retry_count, xnis.page_split_count, xnis.page_split_retry_count,
	                          xnis.key_split_count, xnis.key_split_retry_count, xnis.page_merge_count, xnis.page_merge_retry_count,
	                          xnis.key_merge_count, xnis.key_merge_retry_count,
	                          xis.scans_started, xis.scans_retries
                          FROM sys.dm_db_xtp_nonclustered_index_stats AS xnis (NOLOCK)
                          INNER JOIN sys.dm_db_xtp_index_stats AS xis (NOLOCK) ON xis.[object_id] = xnis.[object_id] AND xis.[index_id] = xnis.[index_id]
                          INNER JOIN sys.indexes AS si (NOLOCK) ON xis.[object_id] = si.[object_id] AND xis.[index_id] = si.[index_id]
                          INNER JOIN sys.objects AS o (NOLOCK) ON si.[object_id] = o.[object_id]
                          INNER JOIN sys.tables AS mst (NOLOCK) ON mst.[object_id] = o.[object_id]
                          INNER JOIN sys.schemas AS t (NOLOCK) ON t.[schema_id] = mst.[schema_id]
                          WHERE o.[type] = ''U''';

                    BEGIN TRY
                        INSERT INTO #tmpXNCIS
                        EXECUTE sp_executesql @sqlcmd;
                    END TRY
                    BEGIN CATCH
                        SET @ErrorMessage
                            = N'      |-Error ' + CONVERT(VARCHAR(20), ERROR_NUMBER())
                              + N' has occurred while analyzing nonclustered hash indexes. Message: ' + ERROR_MESSAGE()
                              + N' (Line Number: ' + CAST(ERROR_LINE() AS VARCHAR(10)) + N')';
                        RAISERROR(@ErrorMessage, 0, 42) WITH NOWAIT;
                    END CATCH;
                END;
            /*ELSE
					BEGIN
						SELECT @ErrorMessage = '    |-Skipping ' + DB_NAME(@dbid) + '. No memory optimized filegroup was found...'
						RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT;
					END;*/
            END;
        END;

        UPDATE #tmpdbs0
        SET isdone = 1
        WHERE [dbid] = @dbid;
    END;
END;

IF EXISTS (SELECT TOP 1 database_id FROM #tmpXIS WHERE isdone = 0)
BEGIN
    --RAISERROR('    |-Gathering additional data on xtp hash indexes...', 10, 1) WITH NOWAIT;
    WHILE
    (SELECT COUNT(database_id)FROM #tmpXIS WHERE isdone = 0) > 0
    BEGIN
        SELECT TOP 1
               @dbid = database_id,
               @objectid = [object_id],
               @indexid = [index_id],
               @schema_name = [schema_name],
               @table_name = [table_name],
               @KeyCols = KeyCols
        FROM #tmpXIS
        WHERE isdone = 0;

        SELECT @sqlcmd
            = N'USE ' + QUOTENAME(DB_NAME(@dbid))
              + N'; SELECT @distinctCntOUT = COUNT(*), @OptimBucketCntOUT = POWER(2,CEILING(LOG(CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END)/LOG(2))) FROM (SELECT DISTINCT '
              + @KeyCols + N' FROM ' + @schema_name + N'.' + @table_name + N') t1;';

        SET @params = N'@distinctCntOUT bigint OUTPUT, @OptimBucketCntOUT bigint OUTPUT';
        EXECUTE sp_executesql @sqlcmd,
                              @params,
                              @distinctCntOUT = @distinctCnt OUTPUT,
                              @OptimBucketCntOUT = @OptimBucketCnt OUTPUT;

        UPDATE #tmpXIS
        SET DistinctCnt = @distinctCnt,
            OptimBucketCnt = @OptimBucketCnt,
            isdone = 1
        WHERE database_id = @dbid
              AND [object_id] = @objectid
              AND [index_id] = @indexid;
    END;
END;

IF @sqlmajorver >= 12
BEGIN
    IF
    (
        SELECT COUNT(*)FROM #tmpXIS WHERE total_bucket_count > DistinctCnt
    ) > 0
    BEGIN
        SELECT 'Check 7 - Xtp Too Many Buckets' AS [Info],
               DB_NAME([database_id]) AS [database_name],
               [schema_name],
               [table_name],
               [index_name],
               [type_desc] AS index_type,
               DistinctCnt AS [distinct_keys],
               OptimBucketCnt AS [optimal_bucket_count],
               total_bucket_count,
               empty_bucket_count,
               FLOOR((CAST(empty_bucket_count AS FLOAT) / total_bucket_count) * 100) AS [empty_bucket_pct],
               avg_chain_length,
               max_chain_length,
               '[WARNING: Some databases have a total bucket count larger than the number of distinct rows in the table, which is wasting memory and marginally slowing down full table scans]' AS [Comment]
        FROM #tmpXIS
        WHERE total_bucket_count > DistinctCnt
        ORDER BY [database_name],
                 [schema_name],
                 table_name,
                 [total_bucket_count] DESC;
    END;
    ELSE
    BEGIN
        SELECT 'Check 7 - Xtp Too Many Buckets' AS [Info],
               '[OK]' AS [Comment];
    END;
END;