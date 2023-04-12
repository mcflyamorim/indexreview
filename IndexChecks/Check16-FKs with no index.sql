/* 
Check16 - Foreign keys without index

Description:
Foreign key constraints should have corresponding indexes. 

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Create a corresponding index on each foreign key.

Detailed recommendation:
A general guideline and a good rule of thumb is to have an associated covered index for any foreign key columns that are commonly used in join operations.
But, the best guidance is “it depends” and is better to base it on the workload.
In a perfect world, you would have to analyze the workload and understand how the data is used, the types of queries and the frequency they run. You would also have to review every single statement and analyze the query execution plan to make sure all required indexes are available and the performance comply with the application user experience expectations, requirements and the reads vs writes tradeoff performance. 
In a real world, is very unlikely that you will have time to analyze case by case. You’ll have to decide whether the impact caused by the “auto-indexed foreign keys” is higher than the benefits, you'll need to do some experiments to find your own sweet spot. In my experience, the cost of having extra indexes is often lower than the benefits it provides.
Some benefits of having indexed foreign keys:
•	Improve joins and table access:
o	Foreign key columns are frequently used in join criteria when the data from related tables is combined in queries by matching the column or columns in the foreign key constraint of one table with the primary or unique key column or columns in the other table. An index enables the database engine to quickly find related data in the foreign key table. However, creating an index is not required, but, the presence of a foreign key relationship between two tables indicates that the two tables have been optimized to be combined in a query that uses the keys as its criteria.
•	Improve delete (modifications) statements: 
o	When you delete a key row, the database engine must check to see if there are any rows which reference the row being deleted. Let’s suppose a classic scenario containing an users table with column user_id being replicated in several tables across the database. So, customers, products, orders and other 200 tables will have an user_id column with a FK pointing back to users table. Now, suppose someone is trying to remove an user and trying to run “delete from users where user_id = 10”, if you’re lucky, you’ll receive an exception with a “The query processor ran out of internal resources and could not produce a query plan.”, but, if you’re not lucky, you’ll have a huge query plan with an access to each table that depends on the user_id. If the foreign keys are not indexed, you’ll have to pay for a scan (that can be very expensive) to check if the user_id you’re trying to remove exists on the related tables, that will include acquire and hold all required locks related to the operation while the transaction is running. So, an index can not also help on performance of select statements, but it can also be very helpful to improve modifications.
•	Trusted foreign keys can help query optimizer to create better plans.

Other considerations:
•	Make sure you’re creating the correct index structure, although an index can help to speed up a query, a seek in a covered index is faster than a seek + lookup, so, make sure that when possible (the overhead is not too high), you are including the correct columns in the index to avoid the expensive and unordered lookup operations. If the optimizer can retrieve all the data it needs from a nonclustered index without having to reference/lookup the underlying table, it will do so and have better performance.
•	This debate is very similar to the “what is the maximum number of indexes I should have in a single table?”, well, it depends, but, it is often a lot better to have indexes then don’t have it. Remember that any data you want to modify has to be found first, so, the extra cost you pay for a modification in a indexed table is often lower than the benefit of find the row you want to modify quickly.
•	Before create an index, check if it is possible to adjust and consolidate any existing index to achieve the same goal.
•	It is always a good practice to analyze the index usage to identify non-used of rarely used indexes. If you decide to create indexes on all foreign keys, after a few days of index usage monitoring, you could identify whether they’re being useful or not and then remove the non-used indexes.
•	If possible, I highly recommend you to create the indexes in a dev/test environment, replay the production workload and measure the performance impact/differences.
•	If your query is joining tables by using columns that does not have a foreign key, it may be helpful to create indexes on those columns for the same reason already mentioned.
Interesting note: MySQL requires foreign keys to be indexed, as per their documentation: “MySQL requires that foreign key columns be indexed; if you create a table with a foreign key constraint but no index on a given column, an index is created.”. I’m not saying this is good, but, just saying.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck16') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck16

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
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFK')
)
    DROP TABLE #tblFK;
IF NOT EXISTS
(
    SELECT [object_id]
    FROM tempdb.sys.objects (NOLOCK)
    WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFK')
)
    CREATE TABLE #tblFK
    (
        [databaseID] INT,
        [DatabaseName] sysname,
        [constraint_name] NVARCHAR(200),
        [parent_schema_name] NVARCHAR(100),
        [parent_table_name] NVARCHAR(200),
        parent_columns NVARCHAR(4000),
        [referenced_schema] NVARCHAR(100),
        [referenced_table_name] NVARCHAR(200),
        referenced_columns NVARCHAR(4000),
        CONSTRAINT PK_FK
            PRIMARY KEY CLUSTERED (
                                      databaseID,
                                      [constraint_name],
                                      [parent_schema_name]
                                  )
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
        = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
           USE ' + QUOTENAME(@dbname)
          + N'
           ;WITH cteFK AS (
           SELECT t.name AS [parent_schema_name],
	           OBJECT_NAME(FKC.parent_object_id) [parent_table_name],
	           OBJECT_NAME(constraint_object_id) AS [constraint_name],
	           t2.name AS [referenced_schema],
	           OBJECT_NAME(referenced_object_id) AS [referenced_table_name],
	           SUBSTRING((SELECT '','' + RTRIM(COL_NAME(k.parent_object_id,parent_column_id)) AS [data()]
		           FROM sys.foreign_key_columns k (NOLOCK)
		           INNER JOIN sys.foreign_keys (NOLOCK) ON k.constraint_object_id = [object_id]
			           AND k.constraint_object_id = FKC.constraint_object_id
		           ORDER BY constraint_column_id
		           FOR XML PATH('''')), 2, 8000) AS [parent_columns],
	           SUBSTRING((SELECT '','' + RTRIM(COL_NAME(k.referenced_object_id,referenced_column_id)) AS [data()]
		           FROM sys.foreign_key_columns k (NOLOCK)
		           INNER JOIN sys.foreign_keys (NOLOCK) ON k.constraint_object_id = [object_id]
			           AND k.constraint_object_id = FKC.constraint_object_id
		           ORDER BY constraint_column_id
		           FOR XML PATH('''')), 2, 8000) AS [referenced_columns]
           FROM sys.foreign_key_columns FKC (NOLOCK)
           INNER JOIN sys.objects o (NOLOCK) ON FKC.parent_object_id = o.[object_id]
           INNER JOIN sys.tables mst (NOLOCK) ON mst.[object_id] = o.[object_id]
           INNER JOIN sys.schemas t (NOLOCK) ON t.[schema_id] = mst.[schema_id]
           INNER JOIN sys.objects so (NOLOCK) ON FKC.referenced_object_id = so.[object_id]
           INNER JOIN sys.tables AS mst2 (NOLOCK) ON mst2.[object_id] = so.[object_id]
           INNER JOIN sys.schemas AS t2 (NOLOCK) ON t2.[schema_id] = mst2.[schema_id]
           WHERE o.type = ''U'' AND so.type = ''U''
           GROUP BY o.[schema_id],so.[schema_id],FKC.parent_object_id,constraint_object_id,referenced_object_id,t.name,t2.name
           ),
           cteIndexCols AS (
           SELECT t.name AS schemaName,
           OBJECT_NAME(mst.[object_id]) AS objectName,
           SUBSTRING(( SELECT '','' + RTRIM(ac.name) FROM sys.tables AS st
	           INNER JOIN sys.indexes AS mi ON st.[object_id] = mi.[object_id]
	           INNER JOIN sys.index_columns AS ic ON mi.[object_id] = ic.[object_id] AND mi.[index_id] = ic.[index_id] 
	           INNER JOIN sys.all_columns AS ac ON st.[object_id] = ac.[object_id] AND ic.[column_id] = ac.[column_id]
	           WHERE i.[object_id] = mi.[object_id] AND i.index_id = mi.index_id AND ic.is_included_column = 0
	           ORDER BY ac.column_id
           FOR XML PATH('''')), 2, 8000) AS KeyCols
           FROM sys.indexes AS i
           INNER JOIN sys.tables AS mst ON mst.[object_id] = i.[object_id]
           INNER JOIN sys.schemas AS t ON t.[schema_id] = mst.[schema_id]
           WHERE i.[type] IN (1,2,5,6) AND i.is_unique_constraint = 0
	           AND mst.is_ms_shipped = 0
           )
           SELECT ' + CONVERT(VARCHAR(8), @dbid) + N' AS Database_ID, ''' + REPLACE(@dbname, CHAR(39), CHAR(95))
          + N''' AS Database_Name, fk.constraint_name AS constraintName,
	           fk.parent_schema_name AS schemaName, fk.parent_table_name AS tableName,
	           REPLACE(fk.parent_columns,'' ,'','','') AS parentColumns, fk.referenced_schema AS referencedSchemaName,
	           fk.referenced_table_name AS referencedTableName, REPLACE(fk.referenced_columns,'' ,'','','') AS referencedColumns
           FROM cteFK fk
           WHERE NOT EXISTS (SELECT 1 FROM cteIndexCols ict 
					           WHERE fk.parent_schema_name = ict.schemaName
						           AND fk.parent_table_name = ict.objectName 
						           AND REPLACE(fk.parent_columns,'' ,'','','') = ict.KeyCols);';
    BEGIN TRY
        INSERT INTO #tblFK
        EXECUTE sp_executesql @sqlcmd;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_MESSAGE() AS ErrorMessage;
        SELECT @ErrorMessage
            = N'Foreign Keys with no Index subsection - Error raised in TRY block in database ' + @dbname + N'. '
              + ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;

    UPDATE #tmpdbs0
    SET isdone = 1
    WHERE [dbid] = @dbid;
END;

CREATE TABLE tempdb.dbo.tmpIndexCheck16
          ([Info] VARCHAR(800),
           [DatabaseName] VARCHAR(800),
           [Constraint_Name] VARCHAR(800),
           [Schema_Name] VARCHAR(800),
           [Table_Name] VARCHAR(800),
           parentColumns VARCHAR(800),
           Referenced_Schema_Name VARCHAR(800),
           Referenced_Table_Name VARCHAR(800),
           referencedColumns VARCHAR(800),
           [Comment] VARCHAR(800),
           CreateIndexCmd VARCHAR(MAX))

IF
(
    SELECT COUNT(*)FROM #tblFK
) > 0
BEGIN
    INSERT INTO tempdb.dbo.tmpIndexCheck16
    SELECT 'Check 16 - Foreign Keys with no Index' AS [Info],
           FK.[DatabaseName] AS [Database_Name],
           constraint_name AS [Constraint_Name],
           FK.parent_schema_name AS [Schema_Name],
           FK.parent_table_name AS [Table_Name],
           FK.parent_columns AS parentColumns,
           FK.referenced_schema AS Referenced_Schema_Name,
           FK.referenced_table_name AS Referenced_Table_Name,
           FK.referenced_columns AS referencedColumns,
           'Some Foreign Key constraints are not supported by an Index. It is recommended to revise these' AS [Comment],
           CreateIndexCmd = 'USE ' + [DatabaseName] + '; ' + 'CREATE INDEX IX_' + REPLACE(constraint_name, ' ', '_')
                            + ' ON ' + QUOTENAME(parent_schema_name) + '.' + QUOTENAME(parent_table_name) + ' (['
                            + REPLACE(REPLACE(parent_columns, ',', '],['), ']]', ']') + ']);'
    FROM #tblFK FK
    ORDER BY [DatabaseName],
             parent_schema_name,
             parent_table_name,
             referenced_schema,
             Referenced_Table_Name;
END;
ELSE
BEGIN
    INSERT INTO tempdb.dbo.tmpIndexCheck16([Info], Comment)
    SELECT 'Check 16 - Foreign Keys with no Index' AS [Info],
           'OK' AS [Comment];
END;

SELECT tmpIndexCheck16.Info,
       tmpIndexCheck16.DatabaseName,
       tmpIndexCheck16.Constraint_Name,
       tmpIndexCheck16.Schema_Name,
       tmpIndexCheck16.Table_Name,
       Tab_GetIndexInfo.Number_Rows AS current_number_of_rows_table,
       tmpIndexCheck16.parentColumns,
       tmpIndexCheck16.Referenced_Schema_Name,
       tmpIndexCheck16.Referenced_Table_Name,
       tmpIndexCheck16.referencedColumns,
       tmpIndexCheck16.Comment,
       tmpIndexCheck16.CreateIndexCmd
FROM tempdb.dbo.tmpIndexCheck16
INNER JOIN tempdb.dbo.Tab_GetIndexInfo
ON tmpIndexCheck16.[DatabaseName] = Tab_GetIndexInfo.Database_Name
AND tmpIndexCheck16.Schema_Name = Tab_GetIndexInfo.Schema_Name
AND tmpIndexCheck16.Table_Name = Tab_GetIndexInfo.Table_Name
AND Tab_GetIndexInfo.Index_ID <= 1
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name