/* 
Check12 - Hypothetical indexes

Description:
Hypothetical indexes can be particularly useful in scenarios where you need to evaluate the impact of adding an index to a large table or a table with a complex query workload.
Hypothetical indexes in SQL Server are not real indexes, they are created for analysis and testing purposes only and can be safely discarded when they are no longer needed.

Estimated Benefit:
Low

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Remove all hypothetical indexes.

Detailed recommendation:
It is generally a good practice to remove hypothetical indexes after they have served their purpose.

*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck12') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck12

DECLARE @sqlcmd NVARCHAR(MAX),
        @params NVARCHAR(600),
        @sqlmajorver INT;
DECLARE @dbid INT,
        @dbname NVARCHAR(1000);
DECLARE @ErrorSeverity INT,
        @ErrorState INT,
        @ErrorMessage NVARCHAR(4000);

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
                WHERE name in (select Database_Name FROM tempdb.dbo.Tab_GetIndexInfo)';
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

IF
(
    SELECT COUNT(id)FROM #tmpdbs0 WHERE isdone = 0
) > 0
BEGIN
    WHILE
    (SELECT COUNT(id)FROM #tmpdbs0 WHERE isdone = 0) > 0
    BEGIN
        SELECT TOP 1
               @dbname = [dbname],
               @dbid = [dbid]
        FROM #tmpdbs0
        WHERE isdone = 0;
        SET @sqlcmd
            = N'USE ' + QUOTENAME(@dbname) + N';
                  SELECT ''' + REPLACE(@dbname, CHAR(39), CHAR(95))
              + N''' AS [DBName], QUOTENAME(t.name), QUOTENAME(o.[name]), i.name, ''INDEX'' 
                  FROM sys.indexes i 
                  INNER JOIN sys.objects o ON o.[object_id] = i.[object_id] 
                  INNER JOIN sys.tables AS mst ON mst.[object_id] = i.[object_id]
                  INNER JOIN sys.schemas AS t ON t.[schema_id] = mst.[schema_id]
                  WHERE i.is_hypothetical = 1';

        BEGIN TRY
            INSERT INTO #tblHypObj
            EXECUTE sp_executesql @sqlcmd;
        END TRY
        BEGIN CATCH
            SELECT ERROR_NUMBER() AS ErrorNumber,
                   ERROR_MESSAGE() AS ErrorMessage;
            SELECT @ErrorMessage
                = N'Hypothetical objects subsection - Error raised in TRY block in database ' + @dbname + N'. '
                  + ERROR_MESSAGE();
            RAISERROR(@ErrorMessage, 16, 1);
        END CATCH;

        UPDATE #tmpdbs0
        SET isdone = 1
        WHERE [dbid] = @dbid;
    END;
END;

UPDATE #tmpdbs0
SET isdone = 0;

CREATE TABLE tempdb.dbo.tmpIndexCheck12
          ([Info] VARCHAR(800),
           [Database_Name] VARCHAR(800),
           [Table_Name] VARCHAR(800),
           [Object_Name] VARCHAR(800),
           [Object_Type] VARCHAR(800),
           [Comment] VARCHAR(800),
           DropCmd VARCHAR(MAX))

IF
(
    SELECT COUNT([Object])FROM #tblHypObj
) > 0
BEGIN
    INSERT INTO tempdb.dbo.tmpIndexCheck12
    SELECT 'Check12 - Hypothetical indexes' AS [Info],
           DBName AS [Database_Name],
           [Table] AS [Table_Name],
           [Object] AS [Object_Name],
           [Type] AS [Object_Type],
           'Warning - Index marked as hypothetical. Hypothetical indexes are created by the Database Tuning Assistant (DTA) during its tests. If a DTA session was interrupted, these indexes may not be deleted. It is recommended to drop these objects as soon as possible' AS [Comment],
           DropCmd = N'USE ' + QUOTENAME(DBName) + N'; DROP INDEX ' + QUOTENAME(Object) + N' ON ' + [Schema] + N'.'
                     + [Table] + N';'
    FROM #tblHypObj
    ORDER BY 2,
             3,
             5;
END;
ELSE
BEGIN
    INSERT INTO tempdb.dbo.tmpIndexCheck12([Info], [Comment])
    SELECT 'Check12 - Hypothetical indexes' AS [Info],
           'OK' AS [Comment];
END;

SELECT * FROM tempdb.dbo.tmpIndexCheck12