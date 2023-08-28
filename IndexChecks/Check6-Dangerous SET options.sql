/* 
Check6 - Dangerous SET options

Description:
Reports SQL module or tables created with ANSI_NULLS and/or QUOTED_IDENTIFIER options set to OFF.

Consider reviewing the need for these options settings, and in case they are not required, 
you should recreate the SQL module using a session that has both these options set to ON.
Even these settings may not currently relate performance problems, they may prevent further 
performance optimizations and usage of features such as:
Indexed views, indexes on computed columns, filtered indexes, query notifications, XML data type methods and/or spatial index operations.

When you're creating and manipulating indexes on computed columns, filtered, or indexed views, you must set these SET options to ON:
ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_PADDING, and ANSI_WARNINGS. 
Set the option NUMERIC_ROUNDABORT to OFF.

If you don't set any one of these options to the required values, INSERT, UPDATE, DELETE, DBCC CHECKDB, and DBCC CHECKTABLE 
actions on filtered, indexed views or tables with indexes on computed columns will fail. 
SQL Server will raise an error listing all the options that are incorrectly set. 
Also, SQL Server will process SELECT statements on these tables or indexed views as 
if the index don't exist.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Recreate objects with recommended SET options.

Detailed recommendation:
Consider reviewing the need for these options settings, and in case they are not required, 
you should recreate the SQL module using a session that has both these options set to ON.
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck6') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck6


IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_Obj2') IS NOT NULL
  DROP TABLE #tmp_Obj2

CREATE TABLE #tmp_Obj2 (DatabaseName NVarChar(MAX),
                        SchemaName   NVarChar(MAX),
                        ObjectName   NVarChar(MAX),
                        TypeDesc     NVarChar(MAX),
                        create_date  DATETIME,
                        modify_date  DATETIME,
                        uses_ansi_nulls        VARCHAR(30),
                        uses_quoted_identifier VARCHAR(30))

SELECT d1.[name] into #db
FROM sys.databases d1
where d1.state_desc = 'ONLINE' and is_read_only = 0
and d1.database_id in (SELECT DISTINCT Database_ID FROM tempdb.dbo.Tab_GetIndexInfo)

DECLARE @SQL VarChar(MAX)
declare @Database_Name sysname
DECLARE @ErrMsg VarChar(8000)

DECLARE c_databases CURSOR read_only FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
into @Database_Name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @SQL = 'use [' + @Database_Name + ']; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
              SELECT 
	               DB_NAME() AS DatabaseName,
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS ObjName,
                t.type_desc AS TypeDesc,
                t.create_date, 
                t.modify_date,
                CONVERT(VARCHAR(30), t.uses_ansi_nulls) AS uses_ansi_nulls,
                CONVERT(VARCHAR(30), ''NA'') AS uses_quoted_identifier
              FROM sys.tables AS t
              WHERE uses_ansi_nulls = 0 /*Default is 1*/
              UNION ALL
              SELECT
                  DB_NAME() AS DatabaseName,
                  SCHEMA_NAME(o.schema_id) AS SchemaName,
                  o.name AS ObjName,
                  o.type_desc AS TypeDesc,
                  o.create_date, 
                  o.modify_date,
                  CONVERT(VARCHAR(30), sm.uses_ansi_nulls) AS uses_ansi_nulls,
                  CONVERT(VARCHAR(30), sm.uses_quoted_identifier) AS uses_quoted_identifier    
              FROM
                  sys.sql_modules AS sm
              INNER JOIN sys.objects AS o
              ON o.[object_id] = sm.[object_id]
              AND o.is_ms_shipped <> 1
              WHERE
                  1 = 1
              AND
              (
                  sm.uses_ansi_nulls = 0
              OR  sm.uses_quoted_identifier = 0
              )
              ORDER BY
                  uses_ansi_nulls,
                  uses_quoted_identifier'

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

SELECT 
'Check6 - Dangerous SET options' AS [Info],
*
INTO tempdb.dbo.tmpIndexCheck6
FROM #tmp_Obj2

SELECT * FROM tempdb.dbo.tmpIndexCheck6