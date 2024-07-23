/*
Check 42 - Report min value for DateTime/Date columns

Description:
Reporting min and max values for date columns for tables greater than 1mi rows.
This is useful to identify tables storing old data and are good candidates to purged/archive/remove, or maybe apply partitioning.

Estimated Benefit:
Very High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Consider to implement a purge/archive strategy or implement partitioning on large tables.

Detailed recommendation:
Review reported tables and implement a purge/archive strategy.
Review reported tables and consider to implement SQL Server native partitioning or partitioned views.
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck42') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck42

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

CREATE TABLE #tmp1 (Database_ID INT, Object_ID INT, Index_ID INT, cMin DATETIME2, cMax DATETIME2, CountMin BIGINT, CountMax BIGINT)
CREATE UNIQUE CLUSTERED INDEX ix1 ON #tmp1(Database_ID, Object_ID, Index_ID)

DECLARE @Database_ID INT, @Object_ID INT, @Index_ID INT, @Cmd NVARCHAR(MAX)
DECLARE @ErrMsg VarChar(8000)

DECLARE c_index1 CURSOR FAST_FORWARD READ_ONLY FOR
SELECT
    Database_ID, Object_ID, Index_ID,
    '
SELECT * FROM (' + 
'
SELECT MIN(' + QUOTENAME(key_column_name) + ') AS cMin, MAX(' + QUOTENAME(key_column_name) + ') AS cMax FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ') AS t' + 
' CROSS APPLY(SELECT COUNT(*) AS CountMin FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' AS t1 WITH(NOLOCK) WHERE t1.' + QUOTENAME(key_column_name) + ' = t.cMin) AS tCountMin' + 
' CROSS APPLY(SELECT COUNT(*) AS CountMax FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' AS t2 WITH(NOLOCK) WHERE t2.' + QUOTENAME(key_column_name) + ' = t.cMax) AS tCountMax' + 
' OPTION(MAXDOP 1);'
AS Cmd
FROM dbo.Tab_GetIndexInfo
WHERE key_column_data_type LIKE '%DATE%'
AND IsTablePartitioned = 0 /* Not reading data from partitioned indexes due to QO limitation to find min/max values from a partitioned table */
AND Number_Rows >= 1000000 /* Only tables >= 1mi rows */

OPEN c_index1

FETCH NEXT FROM c_index1
INTO  @Database_ID, @Object_ID, @Index_ID, @Cmd
WHILE @@FETCH_STATUS = 0
BEGIN
  BEGIN TRY 
    SET @Cmd = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;SET LOCK_TIMEOUT 10; /*10 ms*/; ' + @Cmd

    INSERT INTO #tmp1(cMin, cMax, CountMin, CountMax)
    EXEC (@Cmd)

    UPDATE #tmp1 SET Database_ID = @Database_ID, Object_ID = @Object_ID, Index_ID = @Index_ID
    WHERE Database_ID IS NULL

  END TRY 
  BEGIN CATCH 
    SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to run command ' + @Cmd
    RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

    SET @ErrMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE()
    RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT
  END CATCH;

  FETCH NEXT FROM c_index1
  INTO @Database_ID, @Object_ID, @Index_ID, @Cmd
END
CLOSE c_index1
DEALLOCATE c_index1

SELECT 'Check 42 - Report min value for DateTime/Date columns' AS [Info],
       Database_Name,
       Schema_Name,
       Table_Name,
       Index_Name,
       Index_Type,
       indexed_columns,
       Number_Rows AS current_number_of_rows_table,
       plan_cache_reference_count,
       #tmp1.cMin AS cMin_datetime,
       #tmp1.cMax AS cMax_datetime,
       #tmp1.CountMin,
       #tmp1.CountMax,
       DATEDIFF(YEAR, cMin, cMax) AS YearsCnt,
       key_column_name, 
       key_column_data_type,
'SELECT * FROM (' + '
SELECT MIN(' + QUOTENAME(key_column_name) + ') AS cMin, MAX(' + QUOTENAME(key_column_name) + ') AS cMax FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ') AS t' + 
' CROSS APPLY(SELECT COUNT(*) AS CountMin FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' AS t1 WITH(NOLOCK) WHERE t1.' + QUOTENAME(key_column_name) + ' = t.cMin) AS tCountMin' + 
' CROSS APPLY(SELECT COUNT(*) AS CountMax FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' AS t2 WITH(NOLOCK) WHERE t2.' + QUOTENAME(key_column_name) + ' = t.cMax) AS tCountMax' + 
' OPTION(MAXDOP 1);' AS Cmd
INTO dbo.tmpIndexCheck42
FROM #tmp1
INNER JOIN dbo.Tab_GetIndexInfo
ON Tab_GetIndexInfo.Database_ID = #tmp1.Database_ID
AND Tab_GetIndexInfo.Object_ID = #tmp1.Object_ID
AND Tab_GetIndexInfo.Index_ID = #tmp1.Index_ID

SELECT * FROM dbo.tmpIndexCheck42
 ORDER BY current_number_of_rows_table DESC, 
          Database_Name,
          Schema_Name,
          Table_Name,
          Index_Name