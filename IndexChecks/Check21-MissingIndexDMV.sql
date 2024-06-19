/* 
Check21 - Missing index DMV

Description:
Potentially missing indexes were found based on missing index SQL Server DMVs. It is important to revise them.

Estimated Benefit:
High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review recommended missing indexes and if possible, create them.

Detailed recommendation:
Review missing index suggestions to effectively tune indexes and improve query performance. Review the base table structure, carefully combine indexes, consider key column order, and review included column suggestions, examine missing indexes and existing indexes for overlap and avoid creating duplicate indexes.
It's a best practice to review all the missing index requests for a table and the existing indexes on a table before adding an index based on a query execution plan.
Missing index suggestions are best treated as one of several sources of information when performing index analysis, design, tuning, and testing. Missing index suggestions are not prescriptions to create indexes exactly as suggested.
Review the missing index recommendations for a table as a group, along with the definitions of existing indexes on the table. Remember that when defining indexes, generally equality columns should be put before the inequality columns, and together they should form the key of the index. To determine an effective order for the equality columns, order them based on their selectivity: list the most selective columns first (leftmost in the column list). Unique columns are most selective, while columns with many repeating values are less selective.
It's important to confirm if your index changes have been successful, “is the query optimizer using your indexes?”. Keep in mind that while indexes can dramatically improve query performance but indexes also have overhead and management costs. 
*/



SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck21') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck21

DECLARE @IC NVARCHAR(4000), @ICWI NVARCHAR(4000), @editionCheck BIT

/* Refer to https://docs.microsoft.com/sql/t-sql/functions/serverproperty-transact-sql */	
IF (SELECT SERVERPROPERTY('EditionID')) IN (1804890536, 1872460670, 610778273, -2117995310)	
SET @editionCheck = 1 -- supports enterprise only features
ELSE	
SET @editionCheck = 0; -- does not support enterprise only features
	
IF OBJECT_ID('tempdb.dbo.#IndexCreation') IS NOT NULL
  DROP TABLE #IndexCreation

CREATE TABLE #IndexCreation (
	[database_id] int,
	DBName NVARCHAR(1000),
	[Table] NVARCHAR(255),
	[ix_handle] int,
	[User_Hits_on_Missing_Index] bigint,
	[Estimated_Improvement_Percent] DECIMAL(5,2),
	[Avg_Total_User_Cost] float,
	[Unique_Compiles] bigint,
	[Score] NUMERIC(19,3),
	[KeyCols] NVARCHAR(1000),
	[IncludedCols] NVARCHAR(4000),
	[Ix_Name] NVARCHAR(255),
	[AllCols] NVARCHAR(max),
	[KeyColsOrdered] NVARCHAR(max),
	[IncludedColsOrdered] NVARCHAR(max)
	)

IF OBJECT_ID('tempdb.dbo.#IndexRedundant') IS NOT NULL
  DROP TABLE #IndexRedundant

CREATE TABLE #IndexRedundant (
	DBName NVARCHAR(1000),
	[Table] NVARCHAR(255),
	[Ix_Name] NVARCHAR(255),
	[ix_handle] int,
	[KeyCols] NVARCHAR(1000),
	[IncludedCols] NVARCHAR(4000),
	[Redundant_With] NVARCHAR(255)
	)

INSERT INTO #IndexCreation
SELECT i.database_id,
	m.[name],
	RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3)) AS [Table],
	i.index_handle AS [ix_handle],
	[User_Hits_on_Missing_Index] = (s.user_seeks + s.user_scans),
	s.avg_user_impact, -- Query cost would reduce by this amount in percentage, on average.
	s.avg_total_user_cost, -- Average cost of the user queries that could be reduced by the index in the group.
	s.unique_compiles, -- Number of compilations and recompilations that would benefit from this missing index group.
	(CONVERT(NUMERIC(19,3), s.user_seeks) + CONVERT(NUMERIC(19,3), s.user_scans)) 
		* CONVERT(NUMERIC(19,3), s.avg_total_user_cost) 
		* CONVERT(NUMERIC(19,3), s.avg_user_impact) AS Score, -- The higher the score, higher is the anticipated improvement for user queries.
	CASE WHEN (i.equality_columns IS NOT NULL AND i.inequality_columns IS NULL) THEN i.equality_columns
			WHEN (i.equality_columns IS NULL AND i.inequality_columns IS NOT NULL) THEN i.inequality_columns
			ELSE i.equality_columns + ',' + i.inequality_columns END AS [KeyCols],
	i.included_columns AS [IncludedCols],
	'IX_' + LEFT(RIGHT(RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3)), LEN(RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3))) - (CHARINDEX('.', RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3)), 1)) - 1),
		LEN(RIGHT(RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3)), LEN(RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3))) - (CHARINDEX('.', RIGHT(i.[statement], LEN(i.[statement]) - (LEN(m.[name]) + 3)), 1)) - 1)) - 1) + '_' + CAST(i.index_handle AS NVARCHAR) AS [Ix_Name],
  (SELECT CASE
             WHEN LEN(CSVString) <= 1 THEN
                 NULL
             ELSE
                 LEFT(CSVString, LEN(CSVString) - 1)
         END
  FROM
  (
      SELECT
          (
              SELECT [data()] AS InputData
              FROM
              (
                  SELECT CONVERT(VARCHAR(3), ic.column_id) + N','
                  FROM sys.dm_db_missing_index_details id
                      CROSS APPLY sys.dm_db_missing_index_columns(id.index_handle) ic
                  WHERE id.index_handle = i.index_handle
                  ORDER BY ic.column_id ASC
                  FOR XML PATH(''), TYPE
              ) AS d([data()])
              FOR XML RAW, TYPE
          ).value('/row[1]/InputData[1]', 'NVARCHAR(max)') AS CSVCol
  ) AS XmlRawData(CSVString)),
  (SELECT CASE
               WHEN LEN(CSVString) <= 1 THEN
                   NULL
               ELSE
                   LEFT(CSVString, LEN(CSVString) - 1)
           END
    FROM
    (
        SELECT
            (
                SELECT [data()] AS InputData
                FROM
                (
                    SELECT CONVERT(VARCHAR(3), ic.column_id) + N','
                    FROM sys.dm_db_missing_index_details id
                        CROSS APPLY sys.dm_db_missing_index_columns(id.index_handle) ic
                    WHERE id.index_handle = i.index_handle
                    AND (ic.column_usage = 'EQUALITY' OR ic.column_usage = 'INEQUALITY')
                    ORDER BY ic.column_id ASC
                    FOR XML PATH(''), TYPE
                ) AS d([data()])
                FOR XML RAW, TYPE
            ).value('/row[1]/InputData[1]', 'NVARCHAR(max)') AS CSVCol
    ) AS XmlRawData(CSVString)),
  (SELECT CASE
               WHEN LEN(CSVString) <= 1 THEN
                   NULL
               ELSE
                   LEFT(CSVString, LEN(CSVString) - 1)
           END
    FROM
    (
        SELECT
            (
                SELECT [data()] AS InputData
                FROM
                (
                    SELECT CONVERT(VARCHAR(3), ic.column_id) + N','
                    FROM sys.dm_db_missing_index_details id
                        CROSS APPLY sys.dm_db_missing_index_columns(id.index_handle) ic
                    WHERE id.index_handle = i.index_handle
                      AND ic.column_usage = 'INCLUDE'
                    ORDER BY ic.column_id ASC
                    FOR XML PATH(''), TYPE
                ) AS d([data()])
                FOR XML RAW, TYPE
            ).value('/row[1]/InputData[1]', 'NVARCHAR(max)') AS CSVCol
    ) AS XmlRawData(CSVString))
FROM sys.dm_db_missing_index_details i
INNER JOIN sys.databases m ON i.database_id = m.database_id
INNER JOIN sys.dm_db_missing_index_groups g ON i.index_handle = g.index_handle
INNER JOIN sys.dm_db_missing_index_group_stats s ON s.group_handle = g.index_group_handle
WHERE i.database_id > 4
	
INSERT INTO #IndexRedundant
SELECT I.DBName, I.[Table], I.[Ix_Name], I.[ix_handle], I.[KeyCols], I.[IncludedCols], I2.[Ix_Name]
FROM #IndexCreation I 
INNER JOIN #IndexCreation I2 ON I.[database_id] = I2.[database_id] AND I.[Table] = I2.[Table] AND I.[Ix_Name] <> I2.[Ix_Name]
	AND (((I.KeyColsOrdered <> I2.KeyColsOrdered OR I.[IncludedColsOrdered] <> I2.[IncludedColsOrdered])
		AND ((CASE WHEN I.[IncludedColsOrdered] IS NULL THEN I.KeyColsOrdered ELSE I.KeyColsOrdered + ',' + I.[IncludedColsOrdered] END) = (CASE WHEN I2.[IncludedColsOrdered] IS NULL THEN I2.KeyColsOrdered ELSE I2.KeyColsOrdered + ',' + I2.[IncludedColsOrdered] END)
			OR I.[AllCols] = I2.[AllCols]))
	OR (I.KeyColsOrdered <> I2.KeyColsOrdered AND I.[IncludedColsOrdered] = I2.[IncludedColsOrdered])
	OR (I.KeyColsOrdered = I2.KeyColsOrdered AND I.[IncludedColsOrdered] <> I2.[IncludedColsOrdered]))
WHERE I.[Score] >= 100
	AND I2.[Score] >= 100
GROUP BY I.DBName, I.[Table], I.[Ix_Name], I.[ix_handle], I.[KeyCols], I.[IncludedCols], I2.[Ix_Name]
ORDER BY I.DBName, I.[Table], I.[Ix_Name]

SELECT 'Check21 - Missing index DMV' AS [Info],
       IC.DBName AS [Database_Name],
       IC.[Table] AS [Table_Name],
       CONVERT(BIGINT, [Score]) AS [Score],
       [User_Hits_on_Missing_Index],
       [Estimated_Improvement_Percent],
       [Avg_Total_User_Cost],
       [Unique_Compiles],
       IC.[KeyCols],
       IC.[IncludedCols],
       IC.[Ix_Name] AS [Index_Name],
       SUBSTRING(
       (
           SELECT ',' + IR.[Redundant_With]
           FROM #IndexRedundant IR
           WHERE IC.DBName = IR.DBName
                 AND IC.[Table] = IR.[Table]
                 AND IC.[ix_handle] = IR.[ix_handle]
           ORDER BY IR.[Redundant_With]
           FOR XML PATH('')
       ),
       2,
       8000
                ) AS [Possibly_Redundant_With],
       '[INFORMATION: Potentially missing indexes were found. It may be important to revise these]' AS [Comment],
       CreateIndexCmd = 'USE ' + QUOTENAME(IC.DBName)  + '; IF EXISTS (SELECT name FROM sysindexes WHERE name = N''' +
				                     IC.[Ix_Name] + ''') DROP INDEX ' + IC.[Table] + '.' +
				                     IC.[Ix_Name] + ';' + 'CREATE INDEX ' +
				                     IC.[Ix_Name] + ' ON ' + IC.[Table] + ' (' + IC.[KeyCols] + CASE WHEN @editionCheck = 1 THEN ') WITH (ONLINE = ON);' ELSE ');' END
INTO dbo.tmpIndexCheck21
FROM #IndexCreation IC
--WHERE [Score] >= 1000
ORDER BY IC.DBName, IC.[Table], IC.[Score] DESC, IC.[User_Hits_on_Missing_Index], IC.[Estimated_Improvement_Percent];

SELECT * FROM dbo.tmpIndexCheck21
ORDER BY [Database_Name], [Table_Name], [Score] DESC,[User_Hits_on_Missing_Index], [Estimated_Improvement_Percent];
