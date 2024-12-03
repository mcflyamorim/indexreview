/* 
Check9 - Hard coded indexes

Description:
Some sql modules have fixed hard coded references to indexes. Because the SQL Server query optimizer typically selects the best execution plan for a query, we recommend that hints be used only as a last resort by experienced developers and database administrators. Also, if the referenced index is removed, the module will start to fail.

Estimated Benefit:
Low

Estimated Effort:
Medium

Recommendation:
Quick recommendation:
Avoid to use hard-coded indexes and review if they’re still best access option.

Detailed recommendation:
Review the modules using the fixed indexes and if possible, remove its dependency. 
Review the query execution plan using the forced index and make sure the plan is the best as compared to the query without the index hint.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck9') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck9

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_Obj2') IS NOT NULL
  DROP TABLE #tmp_Obj2

CREATE TABLE #tmp_Obj2 (DatabaseName                    NVarChar(MAX),
                        SchemaName                      NVarChar(MAX),
                        ObjectName                      NVarChar(MAX),
                        [Type of object]                NVarChar(MAX),
                        IndexName                       NVarChar(MAX),
                        [Object code definition]        XML)

SELECT d1.[name] into #db
FROM sys.databases d1
where d1.state_desc = 'ONLINE' and is_read_only = 0
and d1.database_id in (SELECT DISTINCT Database_ID FROM dbo.Tab_GetIndexInfo)

DECLARE @SQL VarChar(MAX)
declare @Database_Name sysname
DECLARE @ErrMsg VarChar(8000)

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

SELECT DISTINCT Database_ID, Table_Name, Index_Name 
INTO #tmp1
FROM dbo.Tab_GetIndexInfo

DECLARE c_databases CURSOR read_only FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
into @Database_Name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @ErrMsg = 'Checking hard-coded indexes on DB - [' + @Database_Name + ']'
  --RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT


  SET @SQL = 'use [' + @Database_Name + ']; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
              IF OBJECT_ID(''tempdb.dbo.#tmpsql_modules'') IS NOT NULL
                DROP TABLE #tmpsql_modules

              SELECT object_id, definition 
              INTO #tmpsql_modules 
              FROM sys.sql_modules AS sm
              WHERE (PATINDEX(''%'' + ''INDEX='' + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0
                     OR
                     PATINDEX(''%'' + ''INDEX ='' + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0)
              OPTION (MAXDOP 1)

              ;WITH CTE_1
              AS
              (
                SELECT DISTINCT Table_Name, Index_Name FROM #tmp1
                WHERE Database_ID = DB_ID()
              )
              SELECT QUOTENAME(DB_NAME()) AS DatabaseName, 
                     QUOTENAME(ss.name) AS [SchemaName], 
                     QUOTENAME(so.name) AS [ObjectName], 
                     so.type_desc [Type of object],
                     t.Index_Name AS IndexName,
                     CONVERT(XML, Tab1.Col1) AS [Object code definition]
              FROM CTE_1 AS t
              INNER JOIN #tmpsql_modules sm
              ON PATINDEX(''%'' + t.Table_Name + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0
              AND PATINDEX(''%'' + t.Index_Name + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 0
              INNER JOIN sys.objects so 
              ON sm.[object_id] = so.[object_id]
              INNER JOIN sys.schemas ss 
              ON so.[schema_id] = ss.[schema_id]
              CROSS APPLY (SELECT CHAR(13)+CHAR(10) + sm.[definition] + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
              WHERE OBJECTPROPERTY(sm.[object_id],''IsMSShipped'') = 0
              OPTION (FORCE ORDER, MAXDOP 1)'

  /*
  SELECT @SQL
  */
  INSERT INTO #tmp_Obj2
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @Database_Name
END
CLOSE c_databases
DEALLOCATE c_databases

SELECT 'Check 9 - Hard coded indexes' AS [Info],
       DatabaseName,
       SchemaName,
       ObjectName,
       IndexName,
       [Type of object],
       'Warning - Some sql objects have hard-coded references to indexes, those objects will fail if you drop the index.' AS [Comment],
       [Object code definition]
INTO dbo.tmpIndexCheck9
FROM #tmp_Obj2

SELECT * FROM dbo.tmpIndexCheck9
