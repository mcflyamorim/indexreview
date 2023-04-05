/* 
Check39 – Identity columns

Description:
Tests that identity columns values are not getting close to the maximum value for the column data type.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review identity columns with low percent of available values. 

Detailed recommendation:
If an identity column is getting close to the limit of the datatype, you need to know so that you can avoid logical problems in your application and SQL Server errors. For example, if you created an IDENTITY column of smallint datatype, if you try to insert more than 32767 rows in the table, you will get the following error:
Server: Msg 8115, Level 16, State 1, Line 1
Arithmetic overflow error converting IDENTITY to data type smallint. Arithmetic overflow occurred.
This is a limitation of the datatype, not the identity.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck39') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck39

IF OBJECT_ID('tempdb.dbo.#IdentityStatus') IS NOT NULL
    DROP TABLE #IdentityStatus;

CREATE TABLE #IdentityStatus
(
    database_name VARCHAR(128),
    table_name VARCHAR(128),
    column_name VARCHAR(128),
    data_type VARCHAR(128),
    last_value BIGINT,
    seed_value BIGINT,
    increment_value BIGINT,
    max_value BIGINT
);

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
    DROP TABLE #db;

SELECT d1.[name]
INTO #db
FROM sys.databases d1
WHERE d1.state_desc = 'ONLINE'
      AND is_read_only = 0
      AND d1.database_id IN
          (
              SELECT DISTINCT database_id FROM tempdb.dbo.Tab_GetIndexInfo
          );

DECLARE @SQL VARCHAR(MAX);
DECLARE @database_name sysname;
DECLARE @ErrMsg VARCHAR(8000);

DECLARE c_databases CURSOR READ_ONLY FOR SELECT [name] FROM #db;
OPEN c_databases;

FETCH NEXT FROM c_databases
INTO @database_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ErrMsg = 'Checking idendity columns on DB - [' + @database_name + ']';
    --RAISERROR(@ErrMsg, 10, 1) WITH NOWAIT;

    SET @SQL
        = 'use [' + @database_name
          + ']; 
              Insert Into #IdentityStatus
              Select DB_NAME() As [database_name]
                  , Object_Name(id.object_id, DB_ID()) As [table_name]
                  , id.name As [column_name]
                  , t.name As [data_type]
                  , Cast(id.last_value As bigint) As [last_value]
                  , Cast(seed_value As bigint)
                  , Cast(increment_value As bigint)
                  , Case 
                      When t.name = ''tinyint''   Then 255 
                      When t.name = ''smallint''  Then 32767 
                      When t.name = ''int''       Then 2147483647 
                      When t.name = ''bigint''    Then 9223372036854775807
                    End As [max_value]
              From sys.identity_columns As id
              Join sys.types As t
                  On id.system_type_id = t.system_type_id
              Where id.last_value Is Not Null';

    /*SELECT @SQL*/
    INSERT INTO #IdentityStatus
    EXEC (@SQL);

    FETCH NEXT FROM c_databases
    INTO @database_name;
END;
CLOSE c_databases;
DEALLOCATE c_databases;

/* Retrieve our results and format it all prettily */
SELECT 'Check39 – Identity columns' AS [Info],
       database_name,
       table_name,
       column_name,
       data_type,
       last_value,
       percent_remaining,
       CASE
           WHEN percent_remaining <= 30 THEN
               'Warning: approaching max limit'
           ELSE
               'OK'
       END AS [Comment],
       QUOTENAME(table_name) + N'.' + QUOTENAME(ic.column_name) + N' is an identity with type ' + ic.data_type
       + N', last value of ' + ISNULL((CONVERT(NVARCHAR(256), CAST(ic.last_value AS DECIMAL(38, 0)), 1)), N'NULL')
       + N', seed of ' + ISNULL((CONVERT(NVARCHAR(256), CAST(ic.seed_value AS DECIMAL(38, 0)), 1)), N'NULL')
       + N', increment of ' + CAST(ic.increment_value AS NVARCHAR(256)) + N', and range of '
       + CASE ic.data_type
             WHEN 'int' THEN
                 N'+/- 2,147,483,647'
             WHEN 'smallint' THEN
                 N'+/- 32,768'
             WHEN 'tinyint' THEN
                 N'0 to 255'
             ELSE
                 'unknown'
         END AS details
INTO tempdb.dbo.tmpIndexCheck39
FROM #IdentityStatus ic
    CROSS APPLY
(
    SELECT CAST(CASE
                    WHEN ic.increment_value >= 0 THEN
                        CASE ic.data_type
                            WHEN 'int' THEN
        (2147483647 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) / 2147483647. * 100
                            WHEN 'smallint' THEN
        (32768 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) / 32768. * 100
                            WHEN 'tinyint' THEN
        (255 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) / 255. * 100
                            ELSE
                                999
                        END
                    ELSE --ic.increment_value is negative
                        CASE ic.data_type
                            WHEN 'int' THEN
                                ABS(-2147483647 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value))
                                / 2147483647. * 100
                            WHEN 'smallint' THEN
                                ABS(-32768 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) / 32768.
                                * 100
                            WHEN 'tinyint' THEN
                                ABS(0 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) / 255. * 100
                            ELSE
                                -1
                        END
                END AS NUMERIC(5, 1)) AS percent_remaining
) AS calc1;

SELECT * FROM tempdb.dbo.tmpIndexCheck39
ORDER BY percent_remaining ASC