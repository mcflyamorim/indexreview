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

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

CREATE TABLE #tmp1 (Database_ID INT, Object_ID INT, Index_ID INT, cMin DATETIME, cMax DATETIME)
CREATE UNIQUE CLUSTERED INDEX ix1 ON #tmp1(Database_ID, Object_ID, Index_ID)

DECLARE @Database_ID INT, @Object_ID INT, @Index_ID INT, @Cmd NVARCHAR(MAX)
DECLARE @ErrMsg VarChar(8000)

DECLARE c_index1 CURSOR FAST_FORWARD READ_ONLY FOR
SELECT
    Database_ID, Object_ID, Index_ID,
    'SELECT MIN(' + QUOTENAME(key_column_name) + ') AS cMin, MAX(' + QUOTENAME(key_column_name) + ') AS cMax FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' OPTION (MAXDOP 1);' AS Cmd
FROM tempdb.dbo.Tab_GetIndexInfo
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

    INSERT INTO #tmp1(cMin, cMax)
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

SELECT 'Check 43 - Report detailed index usage based on last 60 minutes' AS [Info],
       Database_Name,
       Schema_Name,
       Table_Name,
       Index_Name,
       Index_Type,
       indexed_columns,
       Number_Rows AS current_number_of_rows_table,
       #tmp1.cMin,
       #tmp1.cMax,
       DATEDIFF(YEAR, cMin, cMax) AS YearsCnt,
       key_column_name, 
       key_column_data_type,
       'SELECT MIN(' + QUOTENAME(key_column_name) + ') AS cMin, MAX(' + QUOTENAME(key_column_name) + ') AS cMax FROM ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' WITH(NOLOCK) OPTION (MAXDOP 1);' AS Cmd       
INTO tempdb.dbo.tmpIndexCheck43
FROM #tmp1
INNER JOIN tempdb.dbo.Tab_GetIndexInfo
ON Tab_GetIndexInfo.Database_ID = #tmp1.Database_ID
AND Tab_GetIndexInfo.Object_ID = #tmp1.Object_ID
AND Tab_GetIndexInfo.Index_ID = #tmp1.Index_ID

SELECT * FROM tempdb.dbo.tmpIndexCheck43
 ORDER BY current_number_of_rows_table DESC, 
          Database_Name,
          Schema_Name,
          Table_Name,
          Index_Name