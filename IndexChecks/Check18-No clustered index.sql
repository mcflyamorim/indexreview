/* 
Check18 - Tables do not have a clustered index, but have non-clustered index(es)

Description:
In some cases, it may be possible to have a non-clustered created index created in a heap table, as the access through nonclustered indexes are fast than scanning the heap, and the RID (row identifier consisting of the file number, data page number, and slot on the page) is smaller than a clustered index key and an efficient structure.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review heap tables and consider to create a clustered index.

Detailed recommendation:
Review queries used to access the tables, and if possible, create a clustered index on the table.
Review queries used to access the tables, and if necessary, adjust the non-clustered index to add any missing columns that may be accessed via RID lookup operation. In other words, create a covered index.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck18') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck18

DECLARE @sqlcmd NVARCHAR(MAX),
        @params NVARCHAR(600),
        @sqlmajorver INT;
DECLARE @dbid INT,
        @dbname NVARCHAR(1000);
DECLARE @ErrorSeverity INT,
        @ErrorState INT,
        @ErrorMessage NVARCHAR(4000);

IF OBJECT_ID('tempdb.dbo.#tmpdbs0') IS NOT NULL
    DROP TABLE #tmpdbs0;

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
    = N'SELECT database_id, name, is_read_only, [state], 0 FROM sys.databases (NOLOCK) 
                WHERE name in (select Database_Name FROM dbo.Tab_GetIndexInfo)';
INSERT INTO #tmpdbs0
(
    [dbid],
    [dbname],
    is_read_only,
    [state],
    [isdone]
)
EXEC sp_executesql @sqlcmd;


IF OBJECT_ID('tempdb.dbo.#tblIxs3') IS NOT NULL
    DROP TABLE #tblIxs3;

CREATE TABLE #tblIxs3
(
    [Operation] TINYINT,
    [databaseID] INT,
    [DatabaseName] sysname,
    [schemaName] NVARCHAR(100),
    [objectName] NVARCHAR(200),
    [Rows] BIGINT
);

IF OBJECT_ID('tempdb.dbo.#tblIxs4') IS NOT NULL
    DROP TABLE #tblIxs4;

CREATE TABLE #tblIxs4
(
    [databaseID] INT,
    [DatabaseName] sysname,
    [schemaName] NVARCHAR(100),
    [objectName] NVARCHAR(200),
    [CntCols] INT,
    [CntIxs] INT
);

IF OBJECT_ID('tempdb.dbo.#tblIxs5') IS NOT NULL
    DROP TABLE #tblIxs5;

CREATE TABLE #tblIxs5
(
    [databaseID] INT,
    [DatabaseName] sysname,
    [schemaName] NVARCHAR(100),
    [objectName] NVARCHAR(200),
    [indexName] NVARCHAR(200),
    [indexLocation] NVARCHAR(200)
);

UPDATE #tmpdbs0
SET isdone = 0;

UPDATE #tmpdbs0
SET isdone = 1
WHERE [state] <> 0
      OR [dbid] = 2;

WHILE
(SELECT COUNT(id)FROM #tmpdbs0 WHERE isdone = 0) > 0
BEGIN
    --RAISERROR(N'  |-Starting Indexing per Table', 10, 1) WITH NOWAIT;

    SELECT TOP 1
           @dbname = [dbname],
           @dbid = [dbid]
    FROM #tmpdbs0
    WHERE isdone = 0;
    SET @sqlcmd
        = N'USE ' + QUOTENAME(@dbname) + N';
            SELECT 1 AS [Check], ' + CONVERT(VARCHAR(8), @dbid) + N', ''' + REPLACE(@dbname, CHAR(39), CHAR(95))
                      + N''',	
            s.name, t.name, SUM(p.rows)
            FROM sys.indexes AS si (NOLOCK)
            INNER JOIN sys.tables AS t (NOLOCK) ON si.[object_id] = t.[object_id]
            INNER JOIN sys.schemas AS s (NOLOCK) ON s.[schema_id] = t.[schema_id]
            INNER JOIN sys.partitions AS p (NOLOCK) ON  si.[object_id]=p.[object_id] and si.[index_id]=p.[index_id]
            WHERE si.is_hypothetical = 0
            GROUP BY si.[object_id], t.name, s.name
            HAVING COUNT(si.index_id) = 1 AND MAX(si.index_id) = 0
            UNION ALL
            SELECT 2 AS [Check], ' + CONVERT(VARCHAR(8), @dbid) + N', ''' + REPLACE(@dbname, CHAR(39), CHAR(95))
                      + N''',	
            s.name, t.name, SUM(p.rows)
            FROM sys.indexes AS si (NOLOCK) 
            INNER JOIN sys.tables AS t (NOLOCK) ON si.[object_id] = t.[object_id]
            INNER JOIN sys.schemas AS s (NOLOCK) ON s.[schema_id] = t.[schema_id]
            INNER JOIN sys.partitions AS p (NOLOCK) ON  si.[object_id]=p.[object_id] and si.[index_id]=p.[index_id]
            WHERE si.is_hypothetical = 0
            GROUP BY t.name, s.name
            HAVING COUNT(si.index_id) > 1 AND MIN(si.index_id) = 0;';
    BEGIN TRY
        INSERT INTO #tblIxs3
        EXECUTE sp_executesql @sqlcmd;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_MESSAGE() AS ErrorMessage;
        SELECT @ErrorMessage
            = N'Indexing per Table subsection - Error raised in TRY block 1 in database ' + @dbname + N'. '
              + ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;

    SET @sqlcmd
        = N'USE ' + QUOTENAME(@dbname) + N';
            SELECT '          + CONVERT(VARCHAR(8), @dbid) + N', ''' + REPLACE(@dbname, CHAR(39), CHAR(95))
                      + N''',	s.name, t.name, COUNT(c.column_id), 
            (SELECT COUNT(si.index_id) FROM sys.tables AS t2 INNER JOIN sys.indexes AS si ON si.[object_id] = t2.[object_id]
	            WHERE si.index_id > 0 AND si.[object_id] = t.[object_id] AND si.is_hypothetical = 0
	            GROUP BY si.[object_id])
            FROM sys.tables AS t (NOLOCK)
            INNER JOIN sys.columns AS c (NOLOCK) ON t.[object_id] = c.[object_id] 
            INNER JOIN sys.schemas AS s (NOLOCK) ON s.[schema_id] = t.[schema_id]
            GROUP BY s.name, t.name, t.[object_id];';
    BEGIN TRY
        INSERT INTO #tblIxs4
        EXECUTE sp_executesql @sqlcmd;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_MESSAGE() AS ErrorMessage;
        SELECT @ErrorMessage
            = N'Indexing per Table subsection - Error raised in TRY block 2 in database ' + @dbname + N'. '
              + ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;

    SET @sqlcmd
        = N'USE ' + QUOTENAME(@dbname) + N';
            SELECT DISTINCT ' + CONVERT(VARCHAR(8), @dbid) + N', ''' + REPLACE(@dbname, CHAR(39), CHAR(95))
                      + N''', s.name, t.name, i.name, ds.name
            FROM sys.tables AS t (NOLOCK)
            INNER JOIN sys.indexes AS i (NOLOCK) ON t.[object_id] = i.[object_id] 
            INNER JOIN sys.data_spaces AS ds (NOLOCK) ON ds.data_space_id = i.data_space_id
            INNER JOIN sys.schemas AS s (NOLOCK) ON s.[schema_id] = t.[schema_id]
            WHERE t.[type] = ''U''
	            AND i.[type] IN (1,2)
	            AND i.is_hypothetical = 0
	            -- Get partitioned tables
	            AND t.name IN (SELECT ob.name 
			            FROM sys.tables AS ob (NOLOCK)
			            INNER JOIN sys.indexes AS ind (NOLOCK) ON ind.[object_id] = ob.[object_id] 
			            INNER JOIN sys.data_spaces AS sds (NOLOCK) ON sds.data_space_id = ind.data_space_id
			            WHERE sds.[type] = ''PS''
			            GROUP BY ob.name)
	            AND ds.[type] <> ''PS'';';
    BEGIN TRY
        INSERT INTO #tblIxs5
        EXECUTE sp_executesql @sqlcmd;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_MESSAGE() AS ErrorMessage;
        SELECT @ErrorMessage
            = N'Indexing per Table subsection - Error raised in TRY block 3 in database ' + @dbname + N'. '
              + ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;

    UPDATE #tmpdbs0
    SET isdone = 1
    WHERE [dbid] = @dbid;
END;

CREATE TABLE dbo.tmpIndexCheck18
          ([Info] VARCHAR(800),
           [DatabaseName] VARCHAR(800),
           schemaName VARCHAR(800),
           [objectName] VARCHAR(800),
           current_number_of_rows_table BIGINT,
           [Comment] VARCHAR(800))

IF
(
    SELECT COUNT(*)FROM #tblIxs3 WHERE [Operation] = 2
) > 0
BEGIN
    INSERT INTO dbo.tmpIndexCheck18
    SELECT 'Check 18 - Tables do not have a clustered index, but have non-clustered index(es)' AS [Info],
           [DatabaseName] AS [Database_Name],
           schemaName AS [Schema_Name],
           [objectName] AS [Table_Name],
           [Rows] AS current_number_of_rows_table,
           '[WARNING: Some tables do not have a clustered index, but have non-clustered index(es)]' AS [Comment]
    FROM #tblIxs3
    WHERE [Operation] = 2;
END;
ELSE
BEGIN
    INSERT INTO dbo.tmpIndexCheck18 ([Info], [Comment])
    SELECT 'Check 18 - Tables do not have a clustered index, but have non-clustered index(es)' AS [Info],
           'OK' AS [Comment]
END;

SELECT * FROM dbo.tmpIndexCheck18