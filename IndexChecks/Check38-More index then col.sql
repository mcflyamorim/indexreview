/* 
Check38 - More index than Cols

Description:
Poorly designed indexes and a lack of indexes are primary sources of database application bottlenecks. Designing efficient indexes is paramount to achieving good database and application performance.
When a table has a clustered index, the table is called a clustered table. If a table has no clustered index, its data rows are stored in an unordered structure called a heap. If a table is a heap and does not have any nonclustered indexes, then the entire table must be read (a table scan) to find any row.

Estimated Benefit:
High

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Create appropriate indexes to improve performance of queries.

Detailed recommendation:
There are sometimes good reasons to leave a table as a heap instead of creating a clustered index, but using heaps effectively is an advanced skill. Most tables should have a carefully chosen clustered index unless a good reason exists for leaving the table as a heap.
Review all tables with no indexes and make sure they’re really used.
Create appropriate indexes to improve performance of queries.
Usually, some acceptable usages for heaps are:
•	Heaps can be used as staging tables for large, unordered insert operations.
•	Sometimes data professionals also use heaps when data is always accessed through nonclustered indexes, and the RID is smaller than a clustered index key.
•	Very small tables.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck38') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck38

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
    SELECT TOP 1
           @dbname = [dbname],
           @dbid = [dbid]
    FROM #tmpdbs0
    WHERE isdone = 0;
    SET @sqlcmd
        = N'USE ' + QUOTENAME(@dbname) + N'; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
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
        = N'USE ' + QUOTENAME(@dbname) + N'; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
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
        = N'USE ' + QUOTENAME(@dbname) + N'; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
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

CREATE TABLE dbo.tmpIndexCheck38 
          ([Info] VARCHAR(800),
           [DatabaseName] VARCHAR(800),
           schemaName VARCHAR(800),
           [objectName] VARCHAR(800),
           [CntCols] BIGINT,
           [CntIxs] BIGINT,
           [Comment] VARCHAR(800))

IF
(
    SELECT COUNT(*)FROM #tblIxs4 WHERE [CntCols] < [CntIxs]
) > 0
BEGIN
    INSERT INTO dbo.tmpIndexCheck38
    SELECT 'Check38 - More index than Cols' AS [Info],
           [DatabaseName] AS [Database_Name],
           schemaName AS [Schema_Name],
           [objectName] AS [Table_Name],
           [CntCols] AS [Cnt_Columns],
           [CntIxs] AS [Cnt_Indexes],
           '[WARNING: Some tables have more indexes than columns]' AS [Comment]
    FROM #tblIxs4
    WHERE [CntCols] < [CntIxs];
END;
ELSE
BEGIN
    INSERT INTO dbo.tmpIndexCheck38 ([Info], [Comment])
    SELECT 'Check38 - More index than Cols' AS [Info],
           'OK' AS [Comment]
END;

SELECT * FROM dbo.tmpIndexCheck38