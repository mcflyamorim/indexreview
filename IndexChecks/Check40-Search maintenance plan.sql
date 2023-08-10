/*
Check40 - Search for an index maintenance plan

Description:
This check look at modules and jobs to search for an index maintenance plan.

Estimated Benefit:
High

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Make sure you have an index defragmentation maintenance plan.

Detailed recommendation:
If the maintenance plan is not identified, double check there is one, and if not, consider to create it.

For more details about the best index maintenance strategy that balances potential performance improvements against resource consumption required for maintenance check the following article: https://learn.microsoft.com/en-us/sql/relational-databases/indexes/reorganize-and-rebuild-indexes?view=sql-server-ver16#index-maintenance-methods-reorganize-and-rebuild

Note 1: Check the mentioned article for specific recommendations and details about an index maintenance on SQL Azure DB and SQL Managed Instances. An index maintenance may not be necessary in those environments as the rebuild or reorganize operation may degrade performance of other workloads due to resource contention.

Note 2: If available memory is enough to keep all the database pages in cache, fragmentation may be less important, but it is still important to use the available resources as best as possible and avoid extra storage space caused by the internal fragmentation.

Note 3:
Some important notes about why fragmentation still matters even on most modern storage hardware:
- Reading from memory is still a lot faster than reading from any storage (flash based or not) subsystem.
- Low page density (internal fragmentation) will require more pages to store the data, given the cost per gigabyte for high-end storage this could be quite significant.
- Index fragmentation affects the performance of scans and range scans through limiting the size of read-ahead I/Os. This could result in SQL Server not being able to take full advantage of the IOPS and I/O throughput capacity of the storage subsystem. Depending on the storage capability, SQL Server usually achieves a much higher I/O throughput as a direct consequence of requesting large I/Os, as an example, SQL Server can use read-ahead to do up to 8MB in a single I/O request on SQL EE and ColumnStore. It is definitely more efficient to issue 1 x 8-page read than 8 x 1-page reads.
- Index fragmentation can adversely impact execution plan choice: When the Query Optimizer compiles a query plan, it considers the cost of I/O needed to read the data required by the query. With low page density, there are more pages to read, therefore the cost of I/O is higher. This can impact query plan choice. For example, as page density decreases over time due to page splits, the optimizer may compile a different plan for the same query, with a different performance and resource consumption profile.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck40') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck40

DECLARE @dbid int, @dbname VARCHAR(MAX), @sqlcmd NVARCHAR(MAX)
DECLARE @ErrorMessage NVARCHAR(MAX)

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.##tmp1Check40'))
DROP TABLE ##tmp1Check40;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.##tmp1Check40'))
CREATE TABLE ##tmp1Check40 ([DBName] VARCHAR(MAX), [Schema] VARCHAR(MAX), [Object] VARCHAR(MAX), [Type] VARCHAR(MAX), [JobName] VARCHAR(MAX), [is_enabled] BIT, [Step] VARCHAR(MAX), CommandFound VARCHAR(MAX));
		
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblKeywords'))
DROP TABLE #tblKeywords;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblKeywords'))
CREATE TABLE #tblKeywords (
	KeywordID int IDENTITY(1,1) PRIMARY KEY,
	Keyword VARCHAR(64) -- the keyword itself
	);

IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.indexes (NOLOCK) WHERE name = N'UI_Keywords' AND [object_id] = OBJECT_ID('tempdb.dbo.#tblKeywords'))
CREATE UNIQUE INDEX UI_Keywords ON #tblKeywords(Keyword);

INSERT INTO #tblKeywords (Keyword)
VALUES ('ALTER INDEX'), ('DBCC DBREINDEX'), ('REORGANIZE'), ('SHOWCONTIG'), ('INDEXDEFRAG')

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
               WHERE name <> ''tempdb'' and state_desc = ''ONLINE'' and is_read_only = 0';
INSERT INTO #tmpdbs0
(
   [dbid],
   [dbname],
   is_read_only,
   [state],
   [isdone]
)
EXEC sp_executesql @sqlcmd;

UPDATE #tmpdbs0
SET isdone = 0;

IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
BEGIN
	 WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	 BEGIN
		  SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs0 WHERE isdone = 0

		  SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + '; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                   SELECT N''' + REPLACE(@dbname, CHAR(39), CHAR(95)) + ''' AS [DBName], ss.name AS [Schema_Name], so.name AS [Object_Name], so.type_desc, tk.Keyword
                   FROM sys.sql_modules sm (NOLOCK)
                   INNER JOIN sys.objects so (NOLOCK) ON sm.[object_id] = so.[object_id]
                   INNER JOIN sys.schemas ss (NOLOCK) ON so.[schema_id] = ss.[schema_id]
                   CROSS JOIN #tblKeywords tk (NOLOCK)
                   WHERE PATINDEX(''%'' + tk.Keyword + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 1
                   AND OBJECTPROPERTY(sm.[object_id],''IsMSShipped'') = 0;'

    BEGIN TRY
	     INSERT INTO ##tmp1Check40 ([DBName], [Schema], [Object], [Type], CommandFound)
	     EXECUTE sp_executesql @sqlcmd
    END TRY
    BEGIN CATCH
	     SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	     SELECT @ErrorMessage = 'Error raised in TRY block. ' + ERROR_MESSAGE()
	     RAISERROR (@ErrorMessage, 16, 1);
    END CATCH
		
		  UPDATE #tmpdbs0
		  SET isdone = 1
		  WHERE [dbid] = @dbid
	 END
END;

INSERT INTO #tblKeywords (Keyword)
SELECT DISTINCT Object
FROM ##tmp1Check40

SET @sqlcmd = 'USE [msdb]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
               SELECT t.[DBName], t.[Schema], t.[Object], t.[Type], sj.[name], sj.[enabled], sjs.step_name, sjs.[command]
               FROM msdb.dbo.sysjobsteps sjs (NOLOCK)
               INNER JOIN msdb.dbo.sysjobs sj (NOLOCK) ON sjs.job_id = sj.job_id
               CROSS JOIN #tblKeywords tk (NOLOCK)
               OUTER APPLY (SELECT TOP 1 * FROM ##tmp1Check40 WHERE ##tmp1Check40.[Object] = tk.Keyword) AS t
               WHERE PATINDEX(''%'' + tk.Keyword + ''%'', LOWER(sjs.[command]) COLLATE DATABASE_DEFAULT) > 0
               AND sjs.[subsystem] IN (''TSQL'',''PowerShell'', ''CMDEXEC'');'

BEGIN TRY
	 INSERT INTO ##tmp1Check40 ([DBName], [Schema], [Object], [Type], JobName, [is_enabled], Step, CommandFound)
	 EXECUTE sp_executesql @sqlcmd
END TRY
BEGIN CATCH
	 SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	 SELECT @ErrorMessage = 'Error raised in jobs TRY block. ' + ERROR_MESSAGE()
	 RAISERROR (@ErrorMessage, 16, 1);
END CATCH

CREATE TABLE tempdb.dbo.tmpIndexCheck40 (
           [Info] VARCHAR(800),
           DBName VARCHAR(800),
           [Schema] VARCHAR(800),
           [objectName] VARCHAR(800),
           Type VARCHAR(800),
           JobName VARCHAR(800),
           [is_enabled] BIT,
           Step VARCHAR(800),
           CommandFound VARCHAR(MAX),
           [Comment] VARCHAR(800))


IF (SELECT COUNT(*) FROM ##tmp1Check40) > 0
BEGIN
  INSERT INTO tempdb.dbo.tmpIndexCheck40
		SELECT 'Check 40 - Search for an index maintenance plan' AS [Info],
         DBName,
         [Schema],
         Object,
         Type,
         JobName,
         [is_enabled],
         Step,
         CommandFound,
         'OK' AS Comment
  FROM ##tmp1Check40
  WHERE JobName IS NOT NULL
END
ELSE
BEGIN
  INSERT INTO tempdb.dbo.tmpIndexCheck40([Info], Comment)
	 SELECT 'Check 40 - Search for an index maintenance plan' AS [Info],
         'Could not find a job or procedure running index defrag, check manually.'
END;
SELECT * FROM tempdb.dbo.tmpIndexCheck40