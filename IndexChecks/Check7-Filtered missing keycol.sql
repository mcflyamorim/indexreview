/* 
Check7 - Filtered index that may not be used by QO

Description:
Reports indexes when the IS NULL predicate is used with a column that is not included in the index structure.
SQL Server will not use the index when the IS NULL predicate filtering column is not a key column in the index, 
or included column in the filtered index definition.

Estimated Benefit:
High

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Add the missing column to the index definition as a key column or as an included column.

Detailed recommendation:
Consider the following scenario:

You create a filtered index together with the Column IS NULL predicate expression in SQL Server.
The Column field isn't included in the index structure. (That is, the Column field isn't a key 
or included column in the filtered index definition.)

For example, you create the following query:
CREATE UNIQUE CLUSTERED INDEX i_action_rn ON dbo.filter_test (rn)
CREATE NONCLUSTERED INDEX i_action_filt_action_date_type ON dbo.filter_test (action_type)
WHERE action_date IS NULL

The following query doesn't use the filtered index:
SELECT count(*) FROM dbo.filter_test WHERE action_date IS NULL AND action_type=1
*/

SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck7') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck7

IF OBJECT_ID('tempdb.dbo.#tmpSequencial') IS NOT NULL
  DROP TABLE #tmpSequencial

;WITH CTE_1
AS
(
  SELECT 1 AS RowID
  UNION ALL
  SELECT CTE_1.RowID + 1
  FROM CTE_1
  WHERE CTE_1.RowID + 1 <= 1000
)
SELECT RowID
INTO #tmpSequencial
FROM CTE_1
OPTION (MAXRECURSION 1000)


;WITH CTE_1
AS
(
SELECT SUBSTRING(t2.filter_definition, t1.RowID, 1) AS ColTmp1, *,
CASE SUBSTRING(t2.filter_definition, t1.RowID, 1) WHEN '[' THEN 1 WHEN ']' THEN 2 ELSE 0 END ColTmp2
FROM #tmpSequencial t1
CROSS JOIN (SELECT t1.Database_ID, t1.Object_ID, t1.Index_ID, t1.filter_definition
            FROM tempdb.dbo.Tab_GetIndexInfo AS t1
            WHERE t1.has_filter = 1
            AND (t1.filter_definition LIKE '%IS NULL%' OR t1.filter_definition LIKE '%IS NOT NULL%')) AS t2
WHERE t1.RowID <= LEN(t2.filter_definition)
),
CTE_2
AS
(
SELECT a.Database_ID, a.Object_ID, a.Index_ID, a.filter_definition, 
       a.RowID + 1 AS ColStart, 
       (SELECT TOP 1 b.RowID FROM CTE_1 AS b WHERE a.Database_ID = b.Database_ID AND a.Object_ID = b.Object_ID AND a.Index_ID = b.Index_ID AND b.RowID > a.RowID AND b.ColTmp1 = ']' ORDER BY b.RowID ASC) AS ColEnd,
       ((SELECT TOP 1 b.RowID FROM CTE_1 AS b WHERE a.Database_ID = b.Database_ID AND a.Object_ID = b.Object_ID AND a.Index_ID = b.Index_ID AND b.RowID > a.RowID AND b.ColTmp1 = ']' ORDER BY b.RowID ASC)) - (a.RowID + 1) AS ColLen
FROM CTE_1 a
WHERE a.ColTmp1 = '['
)
,
CTE_3
AS
(
SELECT *
 FROM CTE_2
CROSS APPLY(SELECT SUBSTRING(t1.filter_definition, CTE_2.ColStart, CTE_2.ColLen) AS ColName
            FROM tempdb.dbo.Tab_GetIndexInfo AS t1
            WHERE t1.has_filter = 1
            AND (t1.filter_definition LIKE '%IS NULL%' OR t1.filter_definition LIKE '%IS NOT NULL%')
            AND CTE_2.Database_ID = t1.Database_ID AND CTE_2.Object_ID = t1.Object_ID AND CTE_2.Index_ID = t1.Index_ID) AS t2
)
SELECT 'Check7 - Filtered index that may not be used by QO' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.included_columns,
       a.filter_definition,
       QUOTENAME(CTE_3.ColName, '"') AS column_used_on_filter_definition,
       Tab1.is_column_part_of_key_or_included,
       CASE WHEN Tab1.is_column_part_of_key_or_included = 1 THEN 'Warning - SQL Server will not use the index when the IS NULL predicate filtering column is not a key column in the index, or included column in the filtered index definition.' ELSE 'OK' END AS Comment,
       a.Number_Rows AS current_number_of_rows_table,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.plan_cache_reference_count,
       a.ReservedSizeInMB,
       a.Buffer_Pool_SpaceUsed_MB,
       a.Buffer_Pool_FreeSpace_MB,
       CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / CASE WHEN a.Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE a.Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent
INTO tempdb.dbo.tmpIndexCheck7
FROM tempdb.dbo.Tab_GetIndexInfo AS a
INNER JOIN CTE_3
ON CTE_3.Database_ID = a.Database_ID AND CTE_3.Object_ID = a.Object_ID AND CTE_3.Index_ID = a.Index_ID
CROSS APPLY (SELECT CASE 
                      WHEN CONVERT(VARCHAR(MAX), a.indexed_columns) LIKE '%' + QUOTENAME(CTE_3.ColName, '"') + '%' THEN 1
                      WHEN CONVERT(VARCHAR(MAX), a.included_columns) LIKE '%' + QUOTENAME(CTE_3.ColName, '"') + '%' THEN 1
                      ELSE 0
                    END) AS Tab1(is_column_part_of_key_or_included)
WHERE a.has_filter = 1
AND (a.filter_definition LIKE '%IS NULL%' OR a.filter_definition LIKE '%IS NOT NULL%')

SELECT * FROM tempdb.dbo.tmpIndexCheck7
ORDER BY current_number_of_rows_table DESC