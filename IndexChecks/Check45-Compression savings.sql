/*
Check 45 - Estimate compression savings

Description:
Estimate compression savings

Estimated Benefit:
NA

Estimated Effort:
NA

Recommendation:
Quick recommendation:

Detailed recommendation:

ROW compression adds minimal CPU utilization while providing substantial storage and memory savings. For most workloads, ROW compression should be enabled by default for new tables and indexes.
PAGE compression adds higher CPU utilization, but also provides higher storage and memory savings.

Additional analysis and testing may be required to select data compression types optimally. 
Any data compression requires additional CPU processing, thus testing is particularly important when low query latency must be maintained in transactional workloads.

Notes: 

- Writes_Ratio shows the ratio of update/modifications operations on a specific index relative to total operations on that object. 
  The lower the ratio (that is, the table, index, or partition is infrequently updated), the better candidate it is for page compression.
- Don't ignore compression for small objects, consider future growth when deciding what to compress. It is easier to compress a table while it is small than do it once it has become large.

*/

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*if I get blocked for more than 1 sec I'll quit, I don't want to wait or cause other blocks*/
/* ------------------------------------------------------------------------------------------------ */
/* ------------------------------------------------------------------------------------------------ */
/* ------------------------------------ Config params --------------------------------------------- */
/* ------------------------------------------------------------------------------------------------ */
/* ------------------------------------------------------------------------------------------------ */
DECLARE @database_name_filter SYSNAME = '' /*set to null to run script in all DBs*/;
DECLARE @min_rows             BIGINT  = 1000 /*Min of rows on table to be considered on estimation*/;
DECLARE @min_mb               BIGINT  = 100 /*100MB*/ /*Min of MBs on table to be considered on estimation*/;

/*NONE, ROW, PAGE, COLUMNSTORE, COLUMNSTORE_ARCHIVE, COMPRESS*/
--DECLARE @desired_compression  NVARCHAR(500)  = 'NONE, ROW, PAGE, COLUMNSTORE, COLUMNSTORE_ARCHIVE, COMPRESS' /*List of compression to be estimated*/;
DECLARE @desired_compression  NVARCHAR(500)  = 'NONE, ROW, PAGE, COLUMNSTORE, COLUMNSTORE_ARCHIVE, COMPRESS' /*List of compression to be estimated*/;
DECLARE @max_mb_to_sample     NUMERIC(25, 2) = 20 /*Max of MBs to read from source table to be used to populate the temporary object*/
DECLARE @compress_column_size BIGINT= 500 /*Min column size to be considered for COMPRESS test*/

/* Parameters controlling the structure of output scripts: */
DECLARE @online_rebuild	BIT	= 1	/* If 1, will generate REBUILD commands with the ONLINE option turned on */
DECLARE @sort_in_tempdb	BIT	= 1	/* If 1, will generate REBUILD commands with the SORT_IN_TEMPDB option turned on. */
DECLARE @max_dop				    INT	= NULL	/* If not NULL, will add a MaxDOP option accordingly. Set to 1 to prevent parallelism and reduce workload. */
/* ------------------------------------------------------------------------------------------------ */
/* ------------------------------------------------------------------------------------------------ */
/* ------------------------------------ Config params --------------------------------------------- */
/* ------------------------------------------------------------------------------------------------ */
/* ------------------------------------------------------------------------------------------------ */

IF OBJECT_ID ('dbo.tmpIndexCheckMonitoring_Log') IS NULL
BEGIN
  CREATE TABLE dbo.tmpIndexCheckMonitoring_Log (RowID INT IDENTITY(1, 1) PRIMARY KEY, Dt DATETIME DEFAULT GETDATE(), cStatus VARCHAR(MAX))
END

IF OBJECT_ID('dbo.tmpIndexCheck45') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck45

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

IF OBJECT_ID('tempdb.dbo.##tmpIndexCheck45_CompressionResult') IS NOT NULL
  DROP TABLE ##tmpIndexCheck45_CompressionResult;

CREATE TABLE ##tmpIndexCheck45_CompressionResult
(
  [database_name]                                      sysname,
  [object_name]                                        sysname,
  [schema_name]                                        sysname,
  [index_id]                                           INT,
  [index_name]                                         sysname,
  [index_type_desc]                                    NVARCHAR(60),
  [partition_number]                                   INT,
  [estimation_status]                                  NVARCHAR(4000),
  [current_data_compression]                           NVARCHAR(60),
  [estimated_data_compression]                         NVARCHAR(60),
  [compression_ratio]                                  NUMERIC(25, 2),
  [row_count]                                          BIGINT,
  [size_with_current_compression_setting(GB)]          NUMERIC(25, 2),
  [size_with_requested_compression_setting(GB)]        NUMERIC(25, 2),
  [size_compression_saving(GB)]                        NUMERIC(25, 2),
  [size_with_current_compression_setting(KB)]          BIGINT,
  [size_with_requested_compression_setting(KB)]        BIGINT,
  [sample_size_with_current_compression_setting(KB)]   BIGINT,
  [sample_size_with_requested_compression_setting(KB)] BIGINT,
  [sample_compressed_page_count]                       BIGINT,
  [sample_pages_with_current_compression_setting]      BIGINT,
  [sample_pages_with_requested_compression_setting]    BIGINT
);

DECLARE @statusMsg VARCHAR(8000);
DECLARE @sqlmajorver INT
DECLARE @sp_executesql NVARCHAR(MAX)
SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

IF @database_name_filter = ''
  SET @database_name_filter = NULL

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to run script.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db;

CREATE TABLE #db ([name] sysname)

BEGIN TRY
  INSERT INTO #db
  SELECT d1.[name] 
  FROM sys.databases d1
  WHERE 1=1
  AND d1.[name] = ISNULL(@database_name_filter, d1.[name])
  AND d1.[name] IN (SELECT DISTINCT Database_Name FROM dbo.Tab_GetIndexInfo)
  /* I'm not interested to read DBs that are not online :-) */
  AND d1.state_desc = 'ONLINE'
  /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
  AND d1.is_read_only = 0 
  /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
  AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
END TRY
BEGIN CATCH
		SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
  RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);
  SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
  RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);
  RETURN
END CATCH

DECLARE @SQL NVARCHAR(MAX)
DECLARE @Database_Name sysname

DECLARE c_databases CURSOR READ_ONLY FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
INTO @Database_Name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Removing sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + ']'
  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

  SET @sp_executesql = QUOTENAME(@Database_Name) + N'.sys.sp_executesql'

  SET @SQL = 
  '
    IF EXISTS (SELECT *
               FROM [INFORMATION_SCHEMA].[ROUTINES]
               WHERE [ROUTINE_NAME] = ''sp_estimate_data_compression_savings_v2'')
      EXEC (''DROP PROC dbo.sp_estimate_data_compression_savings_v2'');
  '

  BEGIN TRY
    EXECUTE @sp_executesql @stmt = @SQL
    SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to remove sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + ']'
    RAISERROR (@statusMsg, 10, 1) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);
  END TRY
  BEGIN CATCH
    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error while trying to remove sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + '] '
                      + 'ErrMsg = ' + ERROR_MESSAGE();
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

    FETCH NEXT FROM c_databases
    INTO @Database_Name
    CONTINUE;
  END CATCH;

  SET @SQL =
  '
CREATE PROC [dbo].[sp_estimate_data_compression_savings_v2]
(
  @schema_name          sysname,
  @object_name          sysname,
  @index_id             INT            = NULL,
  @partition_number     INT            = NULL,
  @data_compression     NVARCHAR(500),       /*NONE, ROW, PAGE, COLUMNSTORE, COLUMNSTORE_ARCHIVE, COMPRESS*/
  @max_mb_to_sample     NUMERIC(25, 2) = 50,
  @batch_sample_size_mb NUMERIC(25, 2) = 5,
  @compress_column_size BIGINT         = 500 /*Min of column size that shuld be considered for compress, for MAX, use 2147483648*/
)
/*
sp_estimate_data_compression_savings_v2 - April 2023 (v1)

Fabiano Amorim
http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
*/
AS
BEGIN
  SET NOCOUNT ON;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET LOCK_TIMEOUT 1000; /*1 second*/

  DECLARE @status_msg VARCHAR(MAX) = '''';

  IF (SERVERPROPERTY(''EngineEdition'') NOT IN (2 /* Standard */, 3 /* Enterprise */, 4 /* Express */, 8 /*Cloud Lifter */))
  BEGIN
    DECLARE @procName sysname = N''sp_estimate_data_compression_savings_v2'';
    DECLARE @procNameLen INT = DATALENGTH(@procName);

    DECLARE @instanceName sysname = ISNULL(CONVERT(sysname, SERVERPROPERTY(''InstanceName'')), N''MSSQLSERVER'');
    DECLARE @instanceNameLen INT = DATALENGTH(@instanceName);

    RAISERROR(
      ''''''%.*ls'''' failed because it is not supported in the edition of this SQL Server instance ''''%.*ls''''. See books online for more details on feature support in different SQL Server editions.'',
      -1,
      -1,
      @procNameLen,
      @procName,
      @instanceNameLen,
      @instanceName);
    RETURN;
  END;

  DECLARE @sqlmajorver INT;
  SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff);

  /*If not specified, set min value for @compress_column_size*/
  IF (ISNULL(@compress_column_size, 0) = 0)
  BEGIN
    SET @compress_column_size = 100;
  END;
  /*If specified @compress_column_size is greater than 8000 or equal to -1, set to 2147483648*/
  IF (@compress_column_size > 8000)
     OR (@compress_column_size = -1)
  BEGIN
    SET @compress_column_size = 2147483648;
  END;

  IF (ISNULL(@max_mb_to_sample, 0) = 0)
  BEGIN
    RAISERROR(''Error: The input parameter ''''%s'''' has to be greater than zero.'', -1, -1, ''max_mb_to_sample'');
    RETURN;
  END;

  IF (ISNULL(@batch_sample_size_mb, 0) = 0)
  BEGIN
    RAISERROR(''Error: The input parameter ''''%s'''' has to be greater than zero.'', -1, -1, ''batch_sample_size_mb'');
    RETURN;
  END;

  IF (@batch_sample_size_mb > @max_mb_to_sample)
  BEGIN
    RAISERROR(''Error: The input parameter @batch_sample_size_mb has to be lower than @max_mb_to_sample.'', -1, -1);
    RETURN;
  END;

  IF (SUBSTRING(@schema_name, 1, 1) IN (''"'', ''[''))
  BEGIN
    RAISERROR(''Use a non-quoted name for @schema_name(''''%ls'''') parameter.'', -1, -1, @schema_name);
    RETURN;
  END;
  IF (SUBSTRING(@object_name, 1, 1) IN (''"'', ''[''))
  BEGIN
    RAISERROR(''Use a non-quoted name for @object_name(''''%ls'''') parameter.'', -1, -1, @object_name);
    RETURN;
  END;

  -- Check @schema_name parameter
  DECLARE @schema_id INT;
  IF (@schema_name IS NULL)
    SET @schema_id = SCHEMA_ID();
  ELSE
    SET @schema_id = SCHEMA_ID(@schema_name);

  IF (@schema_id IS NULL)
  BEGIN
    RAISERROR(''The schema ''''%ls'''' specified for parameter schema_name does not exist.'', -1, -1, @schema_name);
    RETURN;
  END;
  -- Set the schema name to the default schema
  IF (@schema_name IS NULL)
    SET @schema_name = SCHEMA_NAME(@schema_id);

  -- check object name
  IF (@object_name IS NULL)
  BEGIN
    RAISERROR(''Error: The input parameter ''''%s'''' is not allowed to be null.'', -1, -1, ''object_name'');
    RETURN;
  END;

  -- Check if the object name is a temporary table
  IF (SUBSTRING(@object_name, 1, 1) = ''#'')
  BEGIN
    RAISERROR(
      ''Compressing temporary tables is not supported by the stored procedure sp_estimate_data_compression_savings_v2.'',
      -1,
      -1);
    RETURN;
  END;

  -- Verify that the object exists and that the user has permission to see it.

  DECLARE @fqn NVARCHAR(2000);
  SET @fqn = QUOTENAME(DB_NAME()) + N''.'' + QUOTENAME(@schema_name) + N''.'' + QUOTENAME(@object_name);
  DECLARE @object_id INT = OBJECT_ID(@fqn);

  DECLARE @object_len INT;
  IF (@object_id IS NULL)
  BEGIN
    SET @object_len = DATALENGTH(@fqn);
    RAISERROR(
      ''Cannot find the object "%.*ls" because it does not exist or you do not have permissions.'',
      -1,
      -1,
      @object_len,
      @fqn);
    RETURN;
  END;

  -- Check object type. Must be user table or view.
  IF (NOT EXISTS (SELECT *
                  FROM [sys].[objects]
                  WHERE [object_id] = @object_id
                        AND ([type] = ''U''
                             OR [type] = ''V'')))
  BEGIN
    RAISERROR(''Object ''''%ls'''' does not exist or is not a valid object for this operation.'', -1, -1, @object_name);
    RETURN;
  END;

  -- Check for sparse columns or column sets.
  DECLARE @sparse_columns_and_column_sets INT = (SELECT COUNT(*)
                                                 FROM [sys].[columns]
                                                 WHERE [object_id] = @object_id
                                                       AND ([is_sparse] = 1
                                                            OR [is_column_set] = 1));
  IF (@sparse_columns_and_column_sets > 0)
  BEGIN
    RAISERROR(
      ''Compressing tables with sparse columns or column sets is not supported by the stored procedure sp_estimate_data_compression_savings_v2.'',
      -1,
      -1);
    RETURN;
  END;

  -- check data compression
  IF (@data_compression IS NULL)
  BEGIN
    RAISERROR(''Error: The input parameter ''''%s'''' is not allowed to be null.'', -1, -1, ''@data_compression'');
    RETURN;
  END;

  SET @data_compression = UPPER(@data_compression);
  SET @data_compression = ''<col>'' + REPLACE(@data_compression, '','', ''</col> <col>'') + ''</col>'';

  IF OBJECT_ID(''tempdb.dbo.#tmp_data_compression'') IS NOT NULL
    DROP TABLE [#tmp_data_compression];

  CREATE TABLE [#tmp_data_compression]
  (
    [col_data_compression] NVARCHAR(60)
  );

  INSERT INTO [#tmp_data_compression]
  SELECT RTRIM(LTRIM([tNode_cXML].[value](''.'', ''sysname'')))
  FROM (SELECT CONVERT(XML, @data_compression)) AS [t]([cXML])
  CROSS APPLY [t].[cXML].nodes(''/col'') AS [tNode]([tNode_cXML])
  WHERE ISNULL(RTRIM(LTRIM([tNode_cXML].[value](''.'', ''sysname''))), '''') <> '''';

  DECLARE @col_data_compression NVARCHAR(60);

  DECLARE [c] CURSOR LOCAL FAST_FORWARD FOR
  SELECT [col_data_compression]
  FROM [#tmp_data_compression];
  OPEN [c];

  FETCH NEXT FROM [c]
  INTO @col_data_compression;
  WHILE @@fetch_status = 0
  BEGIN
    IF (@col_data_compression NOT IN (''NONE'', ''ROW'', ''PAGE'', ''COLUMNSTORE'', ''COLUMNSTORE_ARCHIVE'', ''COMPRESS''))
    BEGIN
      RAISERROR(''Invalid value (%ls) specified for %ls parameter.'', -1, -1, @col_data_compression, ''@data_compression'');
      RETURN;
    END;

    FETCH NEXT FROM [c]
    INTO @col_data_compression;
  END;
  CLOSE [c];
  DEALLOCATE [c];

  IF (@index_id IS NOT NULL)
  BEGIN
    DECLARE @index_type INT = NULL;
    SELECT @index_type = [type]
    FROM [sys].[indexes]
    WHERE [object_id] = @object_id
          AND [index_id] = @index_id;

    IF (@index_type IS NULL)
    BEGIN
      RAISERROR(''The selected index does not exist on table ''''%s''''.'', -1, -1, @object_name);
      RETURN;
    END;

    IF (@index_type NOT IN (0, 1, 2, 5, 6))
    BEGIN
      -- Currently do not support XML and spatial, and hash indexes
      RAISERROR(
        ''Compressing XML, spatial, columnstore or hash indexes is not supported by the stored procedure sp_estimate_data_compression_savings_v2.'',
        -1,
        -1);
      RETURN;
    END;
  END;

  IF (@index_id IS NOT NULL)
     AND EXISTS (SELECT *
                 FROM [sys].[indexes]
                 WHERE [object_id] = @object_id
                       AND [index_id] = @index_id
                       AND [is_disabled] = 1)
  BEGIN
    SELECT @status_msg = ''The specified index '' + QUOTENAME([indexes].[name]) + '' on '' + QUOTENAME(@object_name)
                         + '' is disabled.''
    FROM [sys].[indexes]
    WHERE [indexes].[object_id] = @object_id
          AND [indexes].[index_id] = @index_id;

    RAISERROR(@status_msg, 0, 0) WITH NOWAIT;
    RETURN;
  END;

  /* Creating dependency objects */
  IF NOT EXISTS (SELECT *
                 FROM [INFORMATION_SCHEMA].[ROUTINES]
                 WHERE [ROUTINE_NAME] = ''index_review_generate_type'')
    EXEC (''
 create function dbo.index_review_generate_type(
	 @type_id		int, 
	 @type_name		sysname, 
	 @max_length		smallint, 
	 @precision		tinyint, 
	 @scale			tinyint, 
	 @collation_name		sysname,
	 @is_xml_document	bit, 
	 @xml_collection_id	int
 )
 returns nvarchar(max)
 as
 begin
 return '''''''';
 end'');

  EXEC (''alter function dbo.index_review_generate_type
 (
	 @type_id		int, 
	 @type_name		sysname, 
	 @max_length		smallint, 
	 @precision		tinyint, 
	 @scale			tinyint, 
	 @collation_name		sysname,
	 @is_xml_document	bit, 
	 @xml_collection_id	int
 )
 returns nvarchar(max)
 as
 begin
	 return
	 case 
		 when @type_id in (41, 42, 43) -- new date time types
			 then quotename(@type_name) + ''''('''' + convert(nvarchar(10), @scale) + '''')''''
		 when @type_id in (106, 108) -- fixed point numbers
			 then quotename(@type_name) + ''''('''' + convert(nvarchar(10), @precision) + '''','''' + convert(nvarchar(10), @scale) + '''')''''
		 when @type_id in (62) -- floating point numbers where width can be specified
			 then quotename(@type_name) + ''''('''' + convert(nvarchar(10), @precision) + '''')''''
		 when @type_id = 173 -- binary
			 then quotename(@type_name) + ''''('''' + convert(nvarchar(10), @max_length) + '''')''''
		 when @type_id = 165 -- varbinary
			 then quotename(@type_name) + ''''('''' + case @max_length when -1 then ''''max'''' else convert(nvarchar(10), @max_length) end + '''')''''
		 when @type_id in (167, 175) -- ascii char
			 then quotename(@type_name) + ''''('''' + case @max_length when -1 then ''''max'''' else convert(nvarchar(10), @max_length) end + '''') COLLATE '''' + @collation_name
		 when @type_id in (231, 239) -- unicode char
			 then quotename(@type_name) + ''''('''' + case @max_length when -1 then ''''max'''' else convert(nvarchar(10), @max_length / 2) end + '''') COLLATE '''' + @collation_name
		 when @type_id = 241			-- xml
			 then quotename(@type_name) + 
			 case 
				 when @xml_collection_id <> 0
					 then ''''('''' + case when @is_xml_document = 1 then ''''document '''' else '''''''' end + 
						 quotename(''''schema_'''' + convert(nvarchar(10), @xml_collection_id)) + '''')''''
				 else ''''''''
			 end
		 else quotename(@type_name)
	 end
 end'');


  IF NOT EXISTS (SELECT *
                 FROM [INFORMATION_SCHEMA].[ROUTINES]
                 WHERE [ROUTINE_NAME] = ''index_review_column_definition'')
    EXEC (''
 create function dbo.index_review_column_definition
 (
	 @column_name		sysname, 
	 @system_type_id		int, 
	 @system_type_name	sysname,
	 @max_length			smallint, 
	 @precision			tinyint, 
	 @scale				tinyint, 
	 @collation_name		sysname, 
	 @is_nullable		bit, 
	 @is_xml_document	bit, 
	 @xml_collection_id	int, 
	 @is_user_defined	bit, 
	 @is_assembly_type	bit,
	 @is_fixed_length	bit,
  @desired_compression int,
  @compress_column_size bigint
 )
 returns nvarchar(max)
 as
 begin
 return '''''''';
 end'');

  EXEC (''alter function dbo.index_review_column_definition
 (
	 @column_name		sysname, 
	 @system_type_id		int, 
	 @system_type_name	sysname,
	 @max_length			smallint, 
	 @precision			tinyint, 
	 @scale				tinyint, 
	 @collation_name		sysname, 
	 @is_nullable		bit, 
	 @is_xml_document	bit, 
	 @xml_collection_id	int, 
	 @is_user_defined	bit, 
	 @is_assembly_type	bit,
	 @is_fixed_length	bit,
  @desired_compression int,
  @compress_column_size bigint
 )
 returns nvarchar(max)
 begin
	 declare @column_def nvarchar(max)
	 -- Set column name and type
	 set @column_def = quotename(@column_name) + '''' '''' + 
		 case
		 when @is_assembly_type = 1	-- convert assembly to varbinary
			 then case when @is_fixed_length = 1 then ''''[binary]'''' else ''''[varbinary]'''' end + 
			 ''''('''' + case when @max_length = -1 then ''''max'''' else convert(nvarchar(10), @max_length) end + '''')''''
		 when @desired_compression = 5 and (@max_length = -1 or @max_length >= @compress_column_size)
			 then ''''[varbinary](max)''''
		 else   -- what if we we have a user defined type? (like alias)
			 dbo.index_review_generate_type(@system_type_id, @system_type_name, @max_length, @precision, 
						    @scale, @collation_name, @is_xml_document, @xml_collection_id)
		 end
	
	 -- Handle nullability
	 set @column_def = @column_def + case @is_nullable when 1 then '''' NULL'''' else '''' NOT NULL'''' end;

	 return @column_def
 END'');

  IF NOT EXISTS (SELECT *
                 FROM [INFORMATION_SCHEMA].[ROUTINES]
                 WHERE [ROUTINE_NAME] = ''index_review_generate_table_sample_ddl'')
    EXEC (''
 create function dbo.index_review_generate_table_sample_ddl(
	 @object_id int, 
	 @schema sysname, 
	 @table sysname, 
	 @partition_number int, 
	 @partition_column_id int, 
	 @partition_function_id int, 
	 @sample_table_name sysname, 
	 @dummy_column sysname, 
	 @include_computed bit,
	 @sample_percent numeric(25, 4),
  @desired_compression int,
  @compress_column_size bigint
 )
 returns @ddl_statements table(alter_ddl nvarchar(max), insert_ddl nvarchar(max), table_option_ddl nvarchar(max))
 as
 begin
 return;
 end'');

  EXEC (''alter function dbo.index_review_generate_table_sample_ddl(
	 @object_id int, 
	 @schema sysname, 
	 @table sysname, 
	 @partition_number int, 
	 @partition_column_id int, 
	 @partition_function_id int, 
	 @sample_table_name sysname, 
	 @dummy_column sysname, 
	 @include_computed bit,
	 @sample_percent numeric(25, 4),
  @desired_compression int,
  @compress_column_size bigint
 )
 returns @ddl_statements table(alter_ddl nvarchar(max), insert_ddl nvarchar(max), table_option_ddl nvarchar(max))
 as
 begin
	 -- Generate column defintions and select lists
	 declare @column_definitions nvarchar(max);
	 declare @into_list nvarchar(max);
	 declare @columns nvarchar(max);
	
	 --
	 -- if @include_computed is true, we will include non-persisted computed columns as well, this returns all columns in this table.
	 -- For persisted computed column, it''''s always included.
	 --
	 with columns_cte as
	 (
		 select c.column_id, c.name, c.system_type_id, st.name as system_type_name, c.max_length, c.precision, c.scale, c.collation_name,
		    c.is_nullable, c.is_xml_document, c.xml_collection_id, ut.is_user_defined, ut.is_assembly_type, at.is_fixed_length
		 from sys.columns c with (nolock)
		 left join sys.computed_columns cc with (nolock) on c.object_id = cc.object_id and c.column_id = cc.column_id
		 join sys.types ut with (nolock) on c.user_type_id = ut.user_type_id 
		 left join sys.types st with (nolock) on c.system_type_id = st.user_type_id
		 left join sys.assembly_types at with (nolock) on c.user_type_id = at.user_type_id
		 where c.object_id = @object_id 
		   and 1 = case @include_computed when 0 then coalesce(cc.is_persisted, 1) else 1 end
	 )
	 select 
		 -- For example, with a source table definition: create table q ( i int not null, j int not null)
		 -- 
		 -- Below column_definition is generated.
		 --
		 -- -----------------------------------------
		 -- , [i] [int] NOT NULL, [j] [int] NOT NULL
		 --
		 @column_definitions = (
			 select '''', '''' + dbo.index_review_column_definition(name, system_type_id, system_type_name, max_length, precision, scale, 
					 collation_name, is_nullable, is_xml_document, xml_collection_id, is_user_defined, is_assembly_type, is_fixed_length, @desired_compression, @compress_column_size) as [text()] 
			 from columns_cte 
			 order by column_id for xml path(''''''''), type).value(''''.'''', ''''nvarchar(max)''''),
			
		 --
		 -- List of columns to use for insert into, e.g:
		 -- , [i], [j]
		 --
		 @into_list = (
			 select '''', '''' + quotename(name) 
			 from columns_cte where system_type_id <> 189 -- exclude timestamp columns
			 order by column_id for xml path(''''''''), type).value(''''.'''', ''''nvarchar(max)''''),
			
		 --
		 -- List of columns to use for select from, e.g:
		 -- , [i], [j]
		 --
		 @columns = (
			 select '''', '''' + 
				 case 
				 when is_assembly_type = 1 then
					 ''''convert(varbinary(max), '''' + quotename(name) + '''')''''
				 when (@desired_compression = 5) and (system_type_id in (241/*xml*/)) and (max_length = -1 or max_length >= @compress_column_size) then
					 ''''compress(convert(nvarchar(max), '''' + quotename(name) + ''''))''''
				 when (@desired_compression = 5) and (system_type_id in (165/*varbinary*/,167/*varchar*/,173/*binary*/,175/*char*/,231/*nvarchar*/,239/*nchar*/,231/*sysname*/,99/*ntext*/,34/*image*/,35/*text*/)) and (max_length = -1 or max_length >= @compress_column_size) then
					 ''''compress('''' + quotename(name) + '''')''''
				 when xml_collection_id <> 0 then -- untyped the xml and then it will be retyped again
					 ''''convert(xml, '''' + quotename(name) + '''')''''
				 else quotename(name) end 
				 as [text()] 
			 from columns_cte where system_type_id <> 189 -- exclude timestamp columns
			 order by column_id for xml path(''''''''), type).value(''''.'''', ''''nvarchar(max)'''')

	 -- Remove the extra , from the beginning
	 set @column_definitions = stuff(@column_definitions, 1, 2, '''''''');		
	 set @into_list = stuff(@into_list, 1, 2, '''''''');
	 set @columns = stuff(@columns, 1, 2, '''''''');

	 -- Generate ALTER ddl statements
	 declare @alter_ddl nvarchar(max) = ''''''''
	 set @alter_ddl = ''''alter table '''' + quotename(@sample_table_name) + '''' add '''' + @column_definitions + ''''; ''''
	 set @alter_ddl = @alter_ddl + ''''alter table '''' + quotename(@sample_table_name) + '''' drop column '''' + quotename(@dummy_column) + '''';''''

	 -- generate insert ... select statement
	 declare @ddl nvarchar(max) = ''''insert into '''' + quotename(@sample_table_name) + ''''('''' + @into_list + '''')'''' + '''' select '''' + @columns + 
								 '''' from '''' + quotename(@schema) + ''''.'''' + quotename(@table) + '''' tablesample ('''' + convert(nvarchar(max), @sample_percent) + '''' percent)'''';
			

	 --
	 -- NOEXPAND  is a table hint, this table must be an indexed view.
	 --			
	 if (''''V'''' = (select type from sys.objects with(nolock) where object_id = @object_id))
	 begin
		 set @ddl = @ddl + '''' with (noexpand)''''
	 end

	 -- add predicate to filter on partition
	 if @partition_column_id is not null and @partition_function_id is not null
	 begin
		 declare @part_func_name sysname = (select quotename(pf.name) from sys.partition_functions as pf with (nolock) where pf.function_id = @partition_function_id);
		 declare @part_column sysname = (select quotename(name) from sys.columns with (nolock) where object_id = @object_id and column_id = @partition_column_id);

		 set @ddl = @ddl + '''' where $PARTITION.'''' + @part_func_name + ''''('''' + @part_column + '''') = '''' + convert(nvarchar(10), @partition_number);
	
	 end	
	
	 declare @table_option_ddl nvarchar(max) = null;
	 if (''''U'''' = (select type from sys.objects with(nolock) where object_id = @object_id))
	 begin		
		 declare @text_in_row_limit int;
		 declare @large_value_types_out_of_row bit;
		
		 select @text_in_row_limit = text_in_row_limit, @large_value_types_out_of_row = large_value_types_out_of_row
		 from sys.tables with(nolock)
		 where object_id = @object_id;

		 --The ''''text_in_row'''' parameter for sp_tableoption only applies to text, ntext, and image types.  Without one of 
		 --these types, a transaction abort error will be thrown and will cause a deadlock, so we avoid the deadlock here.
		 declare @use_text_in_row as bit;
		 set @use_text_in_row = 0;
		 if (@text_in_row_limit <> 0)
		 begin
			 set @use_text_in_row = 
				 (select count(*) --any non zero value converts bit type to 1
				 from sys.systypes as types with(nolock)
				 inner join (select cols.xtype as xtype from sys.syscolumns as cols with(nolock) inner join sys.sysobjects as objs with(nolock) on cols.id = objs.id where objs.id = @object_id) as coltypes 
				 on coltypes.xtype = types.xtype 
				 where types.name = ''''ntext'''' or types.name = ''''text'''' or types.name = ''''image'''');
		 end
		
		 if (@use_text_in_row <> 0 or @large_value_types_out_of_row <> 0)
		 begin
			 set @table_option_ddl = ''''use tempdb; '''';
			 if (@text_in_row_limit <> 0)
			 begin
				 set @table_option_ddl = @table_option_ddl + ''''exec sp_tableoption '''''''''''' + quotename(@sample_table_name) + '''''''''''', ''''''''text in row'''''''', '''''''''''' + convert(nvarchar(max), @text_in_row_limit) + '''''''''''';'''';
			 end
			
			 if (@large_value_types_out_of_row <> 0)
			 begin
				 set @table_option_ddl = @table_option_ddl + ''''exec sp_tableoption '''''''''''' + quotename(@sample_table_name) + '''''''''''', ''''''''large value types out of row'''''''', ''''''''1'''''''';'''';
			 end
		 end
	 end

	 insert into @ddl_statements values (@alter_ddl, @ddl, @table_option_ddl);

	 return;
 end'');

  IF NOT EXISTS (SELECT *
                 FROM [INFORMATION_SCHEMA].[ROUTINES]
                 WHERE [ROUTINE_NAME] = ''index_review_generate_index_ddl'')
    EXEC (''
 create function dbo.index_review_generate_index_ddl
 (
	 @object_id int, 
	 @index_id int, 
	 @current_compression int,
	 @sample_table sysname,
	 @index_name sysname,
	 @desired_compression int
 )
 returns @ddl_statements table(create_current_index_ddl nvarchar(max), create_desired_index_ddl nvarchar(max), drop_current_index_ddl nvarchar(max),
							 drop_desired_index_ddl nvarchar(max), compress_current_ddl nvarchar(max), compress_desired_ddl nvarchar(max))
 as
 begin
 return;
 end'');

  EXEC (''alter function dbo.index_review_generate_index_ddl
 (
	 @object_id int, 
	 @index_id int, 
	 @current_compression int,
	 @sample_table sysname,
	 @index_name sysname,
	 @desired_compression int
 )
 returns @ddl_statements table(create_current_index_ddl nvarchar(max), create_desired_index_ddl nvarchar(max), drop_current_index_ddl nvarchar(max),
							 drop_desired_index_ddl nvarchar(max), compress_current_ddl nvarchar(max), compress_desired_ddl nvarchar(max))
 as
 begin
	 /*	There are five cases for indexes
			 1) Heap
				 Do not perform additional DDL to create index
				 Use ALTER TABLE DDL to compress table
			 2) Primary Key
				 Use ALTER TABLE DDL to add primary key constraint
				 USE ALTER INDEX DDL to compress index
			 3) Non-PK
				 Use CREATE INDEX DDL to create index
				 USE ALTER INDEX DDL to compress index
			 4) XML Index
				 This should have been filtered out before we got here
			 5) Columnstore Index
				 USE either "ALTER TABLE DDL" or "ALTER INDEX DDL" to compress
		 In all cases, if the index is non-clustered, drop the index */

	 declare @create_current_index_ddl		nvarchar(max) = NULL;
	 declare @create_desired_index_ddl		nvarchar(max) = NULL;
	 declare @drop_current_index_ddl			nvarchar(max) = NULL;
	 declare @drop_desired_index_ddl			nvarchar(max) = NULL;
	 declare @compress_current_ddl			nvarchar(max) = NULL;
	 declare @compress_desired_ddl			nvarchar(max) = NULL;

	 if @index_id = 0		-- HEAP
	 begin
		 --
		 -- Compress the table using the current compression scheme
		 --
		 if @current_compression <> 0
		 begin
			 --
			 -- For base heap, the existing compression type cannot be Columnstore or Columnstore_Archive
			 --
			 set @compress_current_ddl = ''''alter table '''' + quotename(@sample_table) + '''' rebuild with(data_compression = ''''
										 + case @current_compression when 1 then ''''row'''' else ''''page'''' end + '''');'''';
		 end

		 if (@desired_compression not in (3, 4))
		 begin
			 --
			 -- Compress the base heap to desired compression scheme
			 --
			 set @compress_desired_ddl = ''''alter table '''' + quotename(@sample_table) + '''' rebuild with (data_compression = ''''
										 + case @desired_compression when 0 then ''''none'''' when 1 then ''''row'''' else ''''page'''' end + '''');'''';
		 end
		 else
		 begin
			 --
			 -- Create desired index ddl for Columnstore, note that for base heap, we will use clustered columnstore index as targeted index.
			 --
			 set @create_desired_index_ddl = ''''create clustered columnstore index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table);

			 --
			 -- Compress the base heap to desired compression scheme
			 --
			 set @compress_desired_ddl = ''''alter table '''' + quotename(@sample_table) + '''' rebuild with (data_compression = ''''
									   + case @desired_compression when 3 then ''''columnstore'''' else ''''columnstore_archive'''' end + '''');'''';
									  
			 set @drop_desired_index_ddl = ''''drop index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + '''';'''';
		 end
	 end
	 else
	 begin
		 --
		 -- Get Index parameters
		 --
		 declare @is_unique bit, @ignore_dup_key bit, @fill_factor tinyint, @is_padded bit, @index_type tinyint;
		 declare @filter_def nvarchar(max);
		 select @is_unique = i.is_unique, @ignore_dup_key = i.ignore_dup_key,
				 @fill_factor = i.fill_factor, @is_padded = i.is_padded, @filter_def = i.filter_definition, @index_type=i.type
		 from sys.indexes i with (nolock)
		 where i.object_id = @object_id and i.index_id = @index_id;

		 --
		 -- key columns
		 --
		 declare @current_key_columns nvarchar(max);
		 declare @desired_key_columns nvarchar(max);		

		 --
		 -- current/source index type is rowstore,  in which case the key_columns can be constructed with sorting order.
		 --
		 if @index_type not in (5, 6)
		 begin
			 set @current_key_columns  = stuff(
				 (select '''', '''' + quotename(c.name) + case when ic.is_descending_key = 1 then '''' desc'''' else '''' asc'''' end as [text()]
				 from sys.index_columns ic with (nolock) join sys.columns c with (nolock) on ic.object_id = c.object_id and ic.column_id = c.column_id
				 where ic.object_id = @object_id and ic.index_id = @index_id and ic.is_included_column = 0 and ic.key_ordinal <> 0
				 order by ic.key_ordinal
				 for xml path('''''''')), 1, 2, '''''''');

			 if @desired_compression not in (3, 4)
			 begin
				 set @desired_key_columns = @current_key_columns
			 end
			 else	-- desired compress is columnstore, the index should be constructed without any sorting order.
			 begin
				 set @desired_key_columns = stuff(
				 (select '''', '''' + quotename(c.name) as [text()]
				 from sys.index_columns ic with (nolock) join sys.columns c with (nolock) on ic.object_id = c.object_id and ic.column_id = c.column_id
				 where ic.object_id = @object_id and ic.index_id = @index_id and ic.is_included_column = 0 and ic.key_ordinal <> 0
				 order by ic.key_ordinal
				 for xml path('''''''')), 1, 2, '''''''');
			 end			
		 end
		 else	-- current/source index type is columnstore,  in which case the key_columns can be constructed with default sorting order.
		 begin
			 set @current_key_columns  = stuff(
				 (select '''', '''' + quotename(c.name) as [text()]
				 from sys.index_columns ic with (nolock) join sys.columns c with (nolock) on ic.object_id = c.object_id and ic.column_id = c.column_id
				 where ic.object_id = @object_id and ic.index_id = @index_id and ic.is_included_column = 1 and ic.key_ordinal = 0 and ic.is_descending_key = 0
				 order by ic.key_ordinal
				 for xml path('''''''')), 1, 2, '''''''');

			 set @desired_key_columns = @current_key_columns
		 end
		
		 --
		 -- included columns
		 --
		 declare @include_columns nvarchar(max);
		 declare @current_index_include_columns nvarchar(max);
		 declare @desired_index_include_columns nvarchar(max);
		 if (@index_type not in(5, 6))	-- exclude columnstore index as it cannot have included columns
		 begin
			 set @include_columns = stuff(
				 (select '''', '''' + quotename(c.name) as [text()]
				 from sys.index_columns ic with (nolock) join sys.columns c with (nolock) on ic.object_id = c.object_id and ic.column_id = c.column_id
				 where ic.object_id = @object_id and ic.index_id = @index_id and ic.is_included_column = 1
				 order by ic.index_column_id
				 for xml path('''''''')), 1, 2, '''''''');
				
			 set @current_index_include_columns = @include_columns;
			 set @desired_index_include_columns = @include_columns;
			
		 end

		 --
		 -- partition columns -- only those that are not already included in either of the two above
		 -- For non-unique, clustered index/nonclustered columnstore index, partition columns are part of the key
		 -- For non-unique, nonclustered indexes, partition columns can be included
		 --
		 -- For columnstore index, it can also have Columns implicitly added because they are partitioning columns
		 --
		 if (@is_unique = 0)
		 begin
			 declare @partition_column nvarchar(max);
			 select @partition_column = quotename(c.name) 
			 from sys.index_columns ic with (nolock) join sys.columns c with (nolock) on ic.object_id = c.object_id and ic.column_id = c.column_id
			 where ic.object_id = @object_id and ic.index_id = @index_id and ic.is_included_column = 0 and ic.key_ordinal = 0 and ic.partition_ordinal = 1

			 if (@partition_column is not null)
			 begin
				 --
				 --	construct current index key columns
				 --
				 if ((@index_id = 1) or (@index_type = 6))	-- clustered index or nonclustered columnstore index
				 begin										-- clustered columnstore index has empty partition columns to add here, which is fine.
					 set @current_key_columns = coalesce(@current_key_columns + '''', '''' + @partition_column, @partition_column);
				 end				
				 else	-- nonclustered index
				 begin
					 set @current_index_include_columns = coalesce(@current_index_include_columns + '''', '''' + @partition_column, @partition_column);
				 end			
				
				 --
				 --	construct desired key columns
				 --
				 if ((@index_id = 1) or @desired_compression in (3, 4))	-- if desired index is clustered index or a NCCI
				 begin
					 set @desired_key_columns = coalesce(@desired_key_columns + '''', '''' + @partition_column, @partition_column);
				 end
				 else	-- desired index is nonclustered index
				 begin
					 set @desired_index_include_columns = coalesce(@desired_index_include_columns + '''', '''' + @partition_column, @partition_column);
				 end
			 end
		 end;
		
		 begin
			 --
			 -- Within this scope, current_index always will be created, so construct the drop_current_index_ddl first.
			 --
			 set @drop_current_index_ddl = ''''drop index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + '''';'''';

			 --
			 -- Create desired index ddl;
			 --
			 -- Don''''t create desired index if:
			 --	(1). Current index is columnstore and desired compression is columnstore types: columnstore | columnstore_archive.
			 --	(2). Current index is rowstore, and desired compression is rowstore types: None | Row | Page.
			 --
			 -- Similarly for primary key index above.
			 --
			 --if ((@index_type not in (5, 6) and @desired_compression in (3, 4)) or (@index_type in (5, 6) and @desired_compression not in (3, 4)))
			 --begin
				 if (@desired_compression not in(3, 4))	-- Current index_type must be in (5, 6), which is columnstore index, and there is no unique attribute, desired_index_include_columns must also be empty.
				 begin
					 if @index_id <> 1	-- Nonclustered columnstore maps to Nonclustered index.
					 begin
						 set @create_desired_index_ddl = ''''create nonclustered index '''' +	quotename(@index_name) + '''' on '''' + 	-- @Undone, number of key columns converted from NCCI to NCI must not be more than 32
														 quotename(@sample_table) + ''''('''' + @desired_key_columns + '''')'''';	
						 --
						 -- current nonclustered columnstore index may have partitioning columns which was added into include_columns for targeted nonclustered index.
						 --
						 if (@desired_index_include_columns is not null)
						 begin
							 set @create_desired_index_ddl = @create_desired_index_ddl + '''' include ('''' + @desired_index_include_columns + '''')'''';
						 end

						 --set @drop_desired_index_ddl = @drop_current_index_ddl;
       set @drop_desired_index_ddl = ''''drop index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + '''';'''';
					 end
					 --
					 -- else current index is clustered columnstore, which maps to heap, so no need to construct any create_desired_index_ddl.
					 -- reason why we map CCI to heap is because we don''''t know which columns to use as key for a clustered index.
					 --
				 end
				 else	-- Current index must be rowstore, and desired index will be columnstore.
				 begin
					 --
					 -- Map the clustered rowstore index to clustered columnstore index.
					 --

					 if @index_id = 1
					 begin
						 set @create_desired_index_ddl = ''''create clustered columnstore index '''' +
							 quotename(@index_name) + '''' on '''' + quotename(@sample_table);
					 end
					 else
					 begin
						 --
						 -- Map the nonclustered rowstore index to nonclustered columnstore index.
						 --
						 set @create_desired_index_ddl = ''''create nonclustered columnstore index '''' +
							 quotename(@index_name) + '''' on '''' + quotename(@sample_table) + ''''('''' + @desired_key_columns;
						 --
						 -- current index may have included columns, in which case we add it into nonclustered columnstore index.
						 --
						 if (@desired_index_include_columns is not null)
							 set @create_desired_index_ddl = @create_desired_index_ddl + '''', '''' + @desired_index_include_columns + '''')'''';
						 else
							 set @create_desired_index_ddl = @create_desired_index_ddl + '''')'''';
					 end
					
					 --set @drop_desired_index_ddl = @drop_current_index_ddl;
      set @drop_desired_index_ddl = ''''drop index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + '''';'''';
				 end
			 --end

			 if (@create_desired_index_ddl is null)
			 begin
				 set @create_desired_index_ddl = ''''create'''' +
					 case when @is_unique = 1 then '''' /*unique*/'''' else '''''''' end + 
					 case when @index_id = 1 then '''' clustered'''' else '''' nonclustered'''' end +
					 '''' index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + ''''('''' + @desired_key_columns + '''')'''';
					
				 if (@desired_index_include_columns is not null)
					 set @create_desired_index_ddl = @create_desired_index_ddl + '''' include ('''' + @desired_index_include_columns + '''')'''';
			 end

			 --
			 --	create current index ddl;
			 --
			 if (@index_type not in(5, 6))
			 begin
				 set @create_current_index_ddl = ''''create'''' +
					 case when @is_unique = 1 then '''' /*unique*/'''' else '''''''' end + 
					 case when @index_id = 1 then '''' clustered'''' else '''' nonclustered'''' end +
					 '''' index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + ''''('''' + @current_key_columns + '''')'''';
					
				 if (@current_index_include_columns is not null)
					 set @create_current_index_ddl = @create_current_index_ddl + '''' include ('''' + @current_index_include_columns + '''')'''';
			 end
			 else
			 begin
				 set @create_current_index_ddl = ''''create'''' + 
					 case when @index_id = 1 then '''' clustered columnstore'''' else '''' nonclustered columnstore'''' end +
					 '''' index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) +
					 case when @index_id = 1	then ''''''''	else ''''('''' + @current_key_columns + '''')'''' end;
			 end

			 --
			 -- For columnstore, only nonclustered columnstore can have filter_def not null.
			 --
			 if (@filter_def is not null)
			 begin
				 --
				 -- create_desired_index_ddl means desired Hobt is heap; filter_def can only exist in nonclustered rowstore/columnstore index.
				 --
				 if ((@create_desired_index_ddl is not null) and @index_id <> 1)
				 begin
					 set @create_desired_index_ddl = @create_desired_index_ddl + '''' where '''' + @filter_def;
				 end

				 set @create_current_index_ddl = @create_current_index_ddl + '''' where '''' + @filter_def;
			 end
		 end;

		 --
		 -- Append Index Options, skip all options if current index is columnstore type.
		 --
		 -- (1). If the current index is columnstore type, then it won''''t have any of these options, skip appending these options for target index.
		 -- (2). If the current index is rowstore type, create_desired_index_ddl should be either NULL/Empty because there is no need to recreate index if 
		 --		we only care about compare different compression setting on same table/index, or create_desired_index_ddl is to create columnstore, either way, 
		 --		there is no need to append these options.
		 --
		 --if (@index_type not in(5, 6) and (@ignore_dup_key = 1 or @fill_factor <> 0 or @is_padded = 1))
		 --begin
			-- set @create_current_index_ddl  = @create_current_index_ddl + '''' with ('''';
			
			-- declare @requires_comma bit = 0;

			-- --if @ignore_dup_key = 1
			-- --begin
			--	-- set @create_current_index_ddl = @create_current_index_ddl + ''''ignore_dup_key = on'''';
			--	-- set @requires_comma = 1;
			-- --end;

			-- if @fill_factor <> 0
			-- begin
			--	 if @requires_comma = 1 
			--	 begin
			--		 set @create_current_index_ddl = @create_current_index_ddl + '''', '''';
			--	 end
				
			--	 set @create_current_index_ddl = @create_current_index_ddl + ''''fillfactor = '''' + convert(nvarchar(3), @fill_factor);
				
			--	 set @requires_comma = 1;
			-- end;

			-- if @is_padded = 1
			-- begin
			--	 if @requires_comma = 1
			--	 begin
			--		 set @create_current_index_ddl = @create_current_index_ddl + '''', '''';
			--	 end

			--	 set @create_current_index_ddl = @create_current_index_ddl + ''''pad_index = on'''';
			-- end;
			
			-- set @create_current_index_ddl = @create_current_index_ddl + '''')'''';
		 --end;

		 --
		 -- Compress the table/index with current compression.
		 --
		 --if @current_compression <> 0
		 begin
			 if (@index_type not in (5, 6))
			 begin
				 if (@index_id = 1)
					 set @compress_current_ddl = ''''alter table '''' + quotename(@sample_table) + '''' rebuild with(data_compression = '''' + 
						 case @current_compression when 0 then ''''none'''' when 1 then ''''row'''' when 2 then ''''page'''' end + '''')'''';
				 else
					 set @compress_current_ddl = ''''alter index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + 
						 '''' rebuild with(data_compression = '''' + 
						 case @current_compression when 0 then ''''none'''' when 1 then ''''row'''' when 2 then ''''page'''' end + '''')'''';
			 end
			 else
			 begin
				 --
				 -- For both clustered and nonclustered columnstore, using the "Alter index rebuild" DDL is good enough.
				 --
				 set @compress_current_ddl = ''''alter index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + 
					 '''' rebuild with(data_compression = '''' + 
					 case @current_compression when 3 then ''''columnstore'''' else ''''columnstore_archive'''' end + '''')'''';
			 end
		 end;



		 --
		 -- Compress the table/index with desired compression setting.
		 --
		 begin
			 if(@desired_compression not in(3, 4))
			 begin
				 if (@index_id = 1)
					 set @compress_desired_ddl  = ''''alter table '''' + quotename(@sample_table) +
						 '''' rebuild with(data_compression = '''' +
						 case @desired_compression when 0 then ''''none'''' when 1 then ''''row'''' when 2 then ''''page'''' end + '''')'''';
				 else
					 set @compress_desired_ddl = ''''alter index '''' + quotename(@index_name) + '''' on '''' +
						 quotename(@sample_table) + 	'''' rebuild with (data_compression = '''' +
						 case @desired_compression when 0 then ''''none'''' when 1 then ''''row'''' when 2 then ''''page'''' end + '''')'''';
			 end
			 else
			 begin
				 --
				 -- For both clustered and nonclustered columnstore, using the "Alter index rebuild" DDL is enough.
				 --
				 set @compress_desired_ddl = ''''alter index '''' + quotename(@index_name) + '''' on '''' +
					 quotename(@sample_table) + '''' rebuild with (data_compression = '''' +
					 case @desired_compression when 3 then ''''columnstore'''' else ''''columnstore_archive'''' end + '''')'''';
			 end
		 end
	 end;


  set @drop_desired_index_ddl = ''''drop index '''' + quotename(@index_name) + '''' on '''' + quotename(@sample_table) + '''';'''';

		set @create_desired_index_ddl = @create_desired_index_ddl
         + '''' with (data_compression = ''''
									+ case @desired_compression when 0 then ''''none'''' when 1 then ''''row'''' + '''', fillfactor = 100'''' when 2 then ''''page'''' + '''', fillfactor = 100'''' when 3 then ''''columnstore'''' else ''''columnstore_archive'''' end + '''');'''';

		set @create_current_index_ddl = @create_current_index_ddl
         + '''' with (data_compression = ''''
									+ case @current_compression when 0 then ''''none'''' when 1 then ''''row'''' + '''', fillfactor = 100'''' when 2 then ''''page'''' + '''', fillfactor = 100'''' when 3 then ''''columnstore'''' else ''''columnstore_archive'''' end + '''');'''';

  --if @index_type = 1 and @desired_compression not in(3, 4)
  --begin
  --  SET @create_desired_index_ddl = @compress_desired_ddl
  --end

  --if @index_id = 1 and @desired_compression not in(0, 1, 2)
  --begin
  --  SET @create_desired_index_ddl = @compress_desired_ddl
  --end

  if @create_desired_index_ddl is null
  begin
    SET @create_desired_index_ddl = @compress_desired_ddl
  end

  if @create_current_index_ddl is null
  begin
    SET @create_current_index_ddl = @compress_current_ddl
  end

  if @desired_compression = 5
  begin
    SELECT @create_current_index_ddl = NULL, @create_desired_index_ddl = NULL, @drop_current_index_ddl = NULL, @drop_desired_index_ddl = NULL, @compress_current_ddl = NULL, @compress_desired_ddl = NULL
  end

	 insert into @ddl_statements values (@create_current_index_ddl, @create_desired_index_ddl, @drop_current_index_ddl, @drop_desired_index_ddl, @compress_current_ddl, @compress_desired_ddl);

	 return;
 end'');

  /* Finished to create dependency objects */

  -- Hard coded sample table and indexes that we will use
  DECLARE @sample_table NVARCHAR(256) = N''#sample_tableDBA05385A6FF40F888204D05C7D56D2B'';
  DECLARE @dummy_column NVARCHAR(256) = N''dummyDBA05385A6FF40F888204D05C7D56D2B'';
  DECLARE @sample_index NVARCHAR(256) = N''sample_indexDBA05385A6FF40F888204D05C7D56D2B'';
  DECLARE @pages_to_sample INT;

  DECLARE @table_size NUMERIC(25, 2),
          @row_count  BIGINT;
  SELECT @row_count = [row_count],
         @table_size = [table_size]
  FROM (SELECT SUM([row_count]) AS [row_count],
               CONVERT(NUMERIC(25, 2), (SUM([used_page_count]) * 8) / 1024.) AS [table_size]
        FROM [sys].[dm_db_partition_stats]
        WHERE [object_id] = @object_id
              AND [index_id] <= 1
              AND [partition_number] = ISNULL(@partition_number, [partition_number])) AS [t];

  RAISERROR(
    ''------------------------------------------------------------------------------------------------------------------------------------------------'',
    0,
    0) WITH NOWAIT;
  SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                       + ''Starting script execution, working on table '' + QUOTENAME(DB_NAME()) + ''.''
                       + QUOTENAME(@schema_name) + ''.'' + QUOTENAME(@object_name) + ''(RowCount = ''
                       + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, @row_count), 1), ''.00'', '''')
                       + '' | BaseTableSize = '' + CONVERT(VARCHAR(30), @table_size) + ''mb)'';
  RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
  RAISERROR(
    ''------------------------------------------------------------------------------------------------------------------------------------------------'',
    0,
    0) WITH NOWAIT;

  IF @row_count = 0
  BEGIN
    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                         + ''Specified table partition is empty. Skipping this table'';
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
    RETURN;
  END;

  IF @max_mb_to_sample > @table_size
  BEGIN
    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Specified @max_mb_to_sample (''
                         + CONVERT(VARCHAR(30), @max_mb_to_sample)
                         + ''mb) parameter is bigger then base table size, @max_mb_to_sample will be set to ''
                         + CONVERT(VARCHAR(30), @table_size) + ''mb'';
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

    SET @max_mb_to_sample = @table_size;
    IF @batch_sample_size_mb > @max_mb_to_sample
      SET @batch_sample_size_mb = @max_mb_to_sample;
  END;
  SET @pages_to_sample = (@max_mb_to_sample * 1024.) / 8;

  -- Find all the partitions and their partitioning info that we need
  SELECT [i].[index_id],
         [i].[type] AS [index_type],
         [i].[type_desc] AS [index_type_desc],
         ISNULL([i].[name], ''HEAP'') AS [index_name],
         [p].[partition_number],
         [p].[data_compression],
         [p].[data_compression_desc],
         [ic].[column_id] AS [partition_column_id],
         [f].[function_id] AS [partition_function_id],
         1 AS [requires_computed],
         [drop_current_index_ddl],
         [drop_desired_index_ddl],
         [create_current_index_ddl],
         [create_desired_index_ddl],
         [compress_current_ddl],
         [compress_desired_ddl],
         [t_desired_compression].[desired_compression]
  INTO [#index_partition_info]
  FROM [sys].[partitions] [p]
  JOIN [sys].[indexes] [i]
  ON [p].[object_id] = [i].[object_id]
     AND [p].[index_id] = [i].[index_id]
  LEFT JOIN (SELECT * FROM [sys].[index_columns] WHERE [partition_ordinal] = 1) [ic]
  ON [p].[object_id] = [ic].[object_id]
     AND [i].[index_id] = [ic].[index_id]
  LEFT JOIN [sys].[partition_schemes] [ps]
  ON [ps].[data_space_id] = [i].[data_space_id]
  LEFT JOIN [sys].[partition_functions] [f]
  ON [f].[function_id] = [ps].[function_id]
  CROSS APPLY (SELECT CASE [col_data_compression]
                        WHEN ''NONE'' THEN 0
                        WHEN ''ROW'' THEN 1
                        WHEN ''PAGE'' THEN 2
                        WHEN ''COLUMNSTORE'' THEN 3
                        WHEN ''COLUMNSTORE_ARCHIVE'' THEN 4
                        WHEN ''COMPRESS'' THEN 5
                      END
               FROM [#tmp_data_compression]) AS [t_desired_compression]([desired_compression])
  CROSS APPLY [dbo].index_review_generate_index_ddl(
                @object_id,
                [i].[index_id],
                [p].[data_compression],
                @sample_table,
                @sample_index + ''_'' + CONVERT(VARCHAR(30), [i].[index_id]),
                [t_desired_compression].[desired_compression])
  WHERE [p].[object_id] = @object_id
        AND [i].[is_disabled] = 0
        AND [i].[is_hypothetical] = 0
        AND [p].[rows] > 0 -- Ignoring empty partitions 
        --
        -- Filter on index and/or partition if these were provided - always include the clustered index if there is one
        --
        AND [i].[type] <= 6
        AND [i].[type] NOT IN (3, 4) -- ignore XML, Extended indexes for now
        AND ([i].[index_id] = CASE WHEN @index_id IS NULL THEN [i].[index_id] ELSE @index_id END
             OR [i].[index_id] = 1 -- Index_id=1 is always included if exists.
    )
        AND [p].[partition_number] = CASE
                                       WHEN @partition_number IS NULL THEN [p].[partition_number]
                                       ELSE @partition_number
                                     END
  ORDER BY [i].[index_id];

  -- Inserting the current compression to make sure we''ve the correct value for actual size
  INSERT INTO [#index_partition_info]
  SELECT [i].[index_id],
         [i].[type] AS [index_type],
         [i].[type_desc] AS [index_type_desc],
         ISNULL([i].[name], ''HEAP'') AS [index_name],
         [p].[partition_number],
         [p].[data_compression],
         [p].[data_compression_desc],
         [ic].[column_id] AS [partition_column_id],
         [f].[function_id] AS [partition_function_id],
         1 AS [requires_computed],
         [drop_current_index_ddl],
         [drop_desired_index_ddl],
         [create_current_index_ddl],
         [create_desired_index_ddl],
         [compress_current_ddl],
         [compress_desired_ddl],
         [t_desired_compression].[desired_compression]
  FROM [sys].[partitions] [p]
  JOIN [sys].[indexes] [i]
  ON [p].[object_id] = [i].[object_id]
     AND [p].[index_id] = [i].[index_id]
  LEFT JOIN (SELECT * FROM [sys].[index_columns] WHERE [partition_ordinal] = 1) [ic]
  ON [p].[object_id] = [ic].[object_id]
     AND [i].[index_id] = [ic].[index_id]
  LEFT JOIN [sys].[partition_schemes] [ps]
  ON [ps].[data_space_id] = [i].[data_space_id]
  LEFT JOIN [sys].[partition_functions] [f]
  ON [f].[function_id] = [ps].[function_id]
  CROSS APPLY (SELECT CASE [p].data_compression_desc
                        WHEN ''NONE'' THEN 0
                        WHEN ''ROW'' THEN 1
                        WHEN ''PAGE'' THEN 2
                        WHEN ''COLUMNSTORE'' THEN 3
                        WHEN ''COLUMNSTORE_ARCHIVE'' THEN 4
                        WHEN ''COMPRESS'' THEN 5
                      END) AS [t_desired_compression]([desired_compression])
  CROSS APPLY [dbo].index_review_generate_index_ddl(
                @object_id,
                [i].[index_id],
                [p].[data_compression],
                @sample_table,
                @sample_index + ''_'' + CONVERT(VARCHAR(30), [i].[index_id]),
                [t_desired_compression].[desired_compression])
  WHERE [p].data_compression_desc COLLATE DATABASE_DEFAULT NOT IN (SELECT col_data_compression COLLATE DATABASE_DEFAULT
                                                                   FROM #tmp_data_compression)
        AND [p].[object_id] = @object_id
        AND [i].[is_disabled] = 0
        AND [i].[is_hypothetical] = 0
        AND [p].[rows] > 0 -- Ignoring empty partitions
        --
        -- Filter on index and/or partition if these were provided - always include the clustered index if there is one
        --
        AND [i].[type] <= 6
        AND [i].[type] NOT IN (3, 4) -- ignore XML, Extended indexes for now
        AND ([i].[index_id] = CASE WHEN @index_id IS NULL THEN [i].[index_id] ELSE @index_id END
             OR [i].[index_id] = 1 -- Index_id=1 is always included if exists.
    )
        AND [p].[partition_number] = CASE
                                       WHEN @partition_number IS NULL THEN [p].[partition_number]
                                       ELSE @partition_number
                                     END
  ORDER BY [i].[index_id];

  --
  -- If the user requested to estimate compression of a view that isn''t indexed, we will not have anything in #index_partition_info
  --
  IF (0 = (SELECT COUNT(*)FROM [#index_partition_info]))
  BEGIN
    RAISERROR(''Object ''''%ls'''' does not exist or is not a valid object for this operation.'', -1, -1, @object_name);
    RETURN;
  END;

  --
  -- Find all the xml schema collections used by the table
  --
  SELECT ''use tempdb; create xml schema collection ''
         + QUOTENAME(N''schema_'' + CONVERT(NVARCHAR(10), [xml_collection_id])) + '' as N''''''
         + REPLACE(CONVERT(NVARCHAR(MAX), XML_SCHEMA_NAMESPACE([schema_name], [name])), N'''''''', N'''''''''''') + '''''''' AS [create_ddl],
         ''use tempdb; drop xml schema collection '' + QUOTENAME(N''schema_'' + CONVERT(NVARCHAR(10), [xml_collection_id])) AS [drop_ddl]
  INTO [#xml_schema_ddl]
  FROM (SELECT DISTINCT
               [c].[xml_collection_id],
               [xsc].[name],
               [s].[name] AS [schema_name]
        FROM [sys].[columns] [c]
        JOIN [sys].[xml_schema_collections] [xsc]
        ON [c].[xml_collection_id] = [xsc].[xml_collection_id]
        JOIN [sys].[schemas] [s]
        ON [xsc].[schema_id] = [s].[schema_id]
        WHERE [c].[object_id] = @object_id
              AND [c].[xml_collection_id] <> 0) [t];

  --
  -- create required xml schema collections
  --
  DECLARE [c] CURSOR LOCAL FAST_FORWARD FOR
  SELECT [create_ddl],
         [drop_ddl]
  FROM [#xml_schema_ddl];
  OPEN [c];
  DECLARE @create_ddl NVARCHAR(MAX),
          @drop_ddl   NVARCHAR(MAX);
  FETCH NEXT FROM [c]
  INTO @create_ddl,
       @drop_ddl;
  WHILE @@fetch_status = 0
  BEGIN
    BEGIN TRY
      EXEC (@drop_ddl);
    END TRY
    BEGIN CATCH
    END CATCH;

    EXEC (@create_ddl);

    FETCH NEXT FROM [c]
    INTO @create_ddl,
         @drop_ddl;
  END;
  CLOSE [c];
  DEALLOCATE [c];

  -- Create results table
  CREATE TABLE [#estimated_results]
  (
    [database_name]                                      sysname,
    [object_name]                                        sysname,
    [schema_name]                                        sysname,
    [index_id]                                           INT,
    [index_name]                                         sysname,
    [index_type_desc]                                    NVARCHAR(60),
    [partition_number]                                   INT,
    [estimation_status]                                  NVARCHAR(4000),
    [current_data_compression]                           NVARCHAR(60),
    [estimated_data_compression]                         NVARCHAR(60),
    [compression_ratio]                                  NUMERIC(25, 2),
    [row_count]                                          BIGINT,
    [size_with_current_compression_setting(GB)]          NUMERIC(25, 2),
    [size_with_requested_compression_setting(GB)]        NUMERIC(25, 2),
    [size_compression_saving(GB)]                        NUMERIC(25, 2),
    [size_with_current_compression_setting(KB)]          BIGINT,
    [size_with_requested_compression_setting(KB)]        BIGINT,
    [sample_size_with_current_compression_setting(KB)]   BIGINT,
    [sample_size_with_requested_compression_setting(KB)] BIGINT,
    [sample_compressed_page_count]                       BIGINT,
    [sample_pages_with_current_compression_setting]      BIGINT,
    [sample_pages_with_requested_compression_setting]    BIGINT
  );

  --
  -- Outer Loop - Iterate through each unique partition sample
  -- Iteration does not have to be in any particular order, the results table will sort that out
  --
  DECLARE @sample_percent NUMERIC(25, 4);

  DECLARE [c] CURSOR LOCAL FAST_FORWARD FOR
  SELECT [partition_column_id],
         [partition_function_id],
         [partition_number],
         [requires_computed],
         [alter_ddl],
         [insert_ddl],
         [table_option_ddl],
         [sample_percent]
  FROM (SELECT DISTINCT
               [partition_column_id],
               [partition_function_id],
               [partition_number],
               [requires_computed]
        FROM [#index_partition_info]) [t]
  CROSS APPLY (SELECT CASE
                        WHEN [used_page_count] <= @pages_to_sample THEN 100
                        WHEN @max_mb_to_sample = @table_size THEN 100
                        ELSE 100. * @pages_to_sample / [used_page_count]
                      END AS [sample_percent]
               FROM [sys].[dm_db_partition_stats] [ps]
               WHERE [ps].[object_id] = @object_id
                     AND [index_id] < 2
                     AND [ps].[partition_number] = [t].[partition_number]) [ps]
  CROSS APPLY [dbo].index_review_generate_table_sample_ddl(
                @object_id,
                @schema_name,
                @object_name,
                [partition_number],
                [partition_column_id],
                [partition_function_id],
                @sample_table,
                @dummy_column,
                [requires_computed],
                [sample_percent],
                NULL,
                @compress_column_size);
  OPEN [c];

  DECLARE @curr_partition_column_id   INT,
          @curr_partition_function_id INT,
          @curr_partition_number      INT,
          @requires_computed          BIT,
          @alter_ddl                  NVARCHAR(MAX),
          @insert_ddl                 NVARCHAR(MAX),
          @table_option_ddl           NVARCHAR(MAX);
  FETCH NEXT FROM [c]
  INTO @curr_partition_column_id,
       @curr_partition_function_id,
       @curr_partition_number,
       @requires_computed,
       @alter_ddl,
       @insert_ddl,
       @table_option_ddl,
       @sample_percent;

  WHILE @@fetch_status = 0
  BEGIN
    RAISERROR(
      ''------------------------------------------------------------------------------------------------------------------------------------------------'',
      0,
      0) WITH NOWAIT;

    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Starting to populate table sample - '' + @fqn + (''(partition_number = '' + CONVERT(VARCHAR(30), @curr_partition_number) + '')'');
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

    -- Step 1. Create the sample table in current scope
    -- 
    CREATE TABLE [#sample_tableDBA05385A6FF40F888204D05C7D56D2B]
    (
      [dummyDBA05385A6FF40F888204D05C7D56D2B] INT
    );

    -- Step 2. Add columns into sample table
    -- 
    EXEC (@alter_ddl);

    ALTER TABLE [#sample_tableDBA05385A6FF40F888204D05C7D56D2B] REBUILD;

    EXEC (@table_option_ddl);

    DECLARE @sample_table_object_id INT = OBJECT_ID(''tempdb.dbo.#sample_tableDBA05385A6FF40F888204D05C7D56D2B'');

    DECLARE @number_percent_sample_batch NUMERIC(25, 4);
    DECLARE @number_sample_batch INT = 1;
    DECLARE @i INT = 1;
    DECLARE @current_size_batch NUMERIC(25, 2) = 0;

    SET @number_sample_batch = @max_mb_to_sample / @batch_sample_size_mb;

    IF @sample_percent = 100.00
    BEGIN
      SET @number_percent_sample_batch = 100.00;
    END;
    ELSE
    BEGIN
      SET @number_percent_sample_batch = @sample_percent / @number_sample_batch;
    END;

    SET @insert_ddl = REPLACE(
                        @insert_ddl,
                        ''tablesample ('' + CONVERT(VARCHAR(30), @sample_percent) + '' percent)'',
                        ''tablesample ('' + CONVERT(VARCHAR(30), @number_percent_sample_batch) + '' percent)'');

    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                         + ''Populating table sample, current table size = 0'' + ''mb (target = ''
                         + CONVERT(VARCHAR(30), @max_mb_to_sample) + ''mb), ''
                         + ''insert_dll = '' + @insert_ddl;
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

    -- @insert_ddl
    EXEC (@insert_ddl);

    SELECT @current_size_batch = CONVERT(NUMERIC(25, 2), ([used_page_count] * 8) / 1024.)
    FROM [tempdb].[sys].[dm_db_partition_stats]
    WHERE [object_id] = @sample_table_object_id;

    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                         + ''Populating table sample, current table size = '' + CONVERT(VARCHAR(30), @current_size_batch)
                         + ''mb (target = '' + CONVERT(VARCHAR(30), @max_mb_to_sample) + ''mb), ''
                         + ''insert_dll = '' + @insert_ddl;
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

    IF @sample_percent = 100.00
       OR @max_mb_to_sample = @batch_sample_size_mb
    BEGIN
      SET @i = @number_sample_batch + 1;
    END;

    WHILE (@i <= @number_sample_batch)
          AND (@current_size_batch < @max_mb_to_sample)
    BEGIN
      -- @insert_ddl
      -- 
      EXEC (@insert_ddl);

      SELECT @current_size_batch = CONVERT(NUMERIC(25, 2), ([used_page_count] * 8) / 1024.)
      FROM [tempdb].[sys].[dm_db_partition_stats]
      WHERE [object_id] = @sample_table_object_id;

      SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                           + ''Populating table sample, current table size = ''
                           + CONVERT(VARCHAR(30), @current_size_batch) + ''mb (target = ''
                           + CONVERT(VARCHAR(30), @max_mb_to_sample) + ''mb), ''
                           + ''insert_dll = '' + @insert_ddl;
      RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

      -- Break if we hit the max_mb_to_sample limit size
      IF @current_size_batch >= @max_mb_to_sample
        BREAK;

      SET @i = @i + 1;
    END;

    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Finished to populate table sample - '' + @fqn + (''(partition_number = '' + CONVERT(VARCHAR(30), @curr_partition_number) + '')'');
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

    --
    -- Step 3.   Inner Loop:
    --			 Iterate through the indexes that use this sampled partition
    --
    DECLARE @index_name sysname;
    DECLARE @curr_index_type_desc NVARCHAR(60);
    DECLARE @curr_index_type INT;
    DECLARE @desired_compression INT;

    DECLARE [index_partition_cursor] CURSOR LOCAL FAST_FORWARD FOR
    SELECT [ipi].[index_id],
           [ipi].[data_compression],
           [ipi].[index_type],
           [ipi].[index_type_desc],
           [ipi].[drop_current_index_ddl],
           [ipi].[drop_desired_index_ddl],
           [ipi].[create_current_index_ddl],
           [ipi].[create_desired_index_ddl],
           [ipi].[compress_current_ddl],
           [ipi].[compress_desired_ddl],
           [ipi].[index_name],
           [ipi].[desired_compression],
           CASE
             WHEN ROW_NUMBER() OVER (PARTITION BY [ipi].[index_id]
                                     ORDER BY [ipi].[desired_compression] ASC) = 1 THEN 1
             ELSE 0
           END AS v_control_current_index_creation
    FROM [#index_partition_info] [ipi]
    WHERE ([ipi].[partition_column_id] = @curr_partition_column_id
           OR ([ipi].[partition_column_id] IS NULL
               AND @curr_partition_column_id IS NULL))
          AND ([partition_function_id] = @curr_partition_function_id
               OR ([partition_function_id] IS NULL
                   AND @curr_partition_function_id IS NULL))
          AND ([ipi].[partition_number] = @curr_partition_number
               OR ([ipi].[partition_number] IS NULL
                   AND @curr_partition_number IS NULL))
          AND [ipi].[requires_computed] = @requires_computed
    ORDER BY [ipi].[index_id] ASC,
             [ipi].[index_type] ASC,
             [ipi].[partition_number] ASC,
             [ipi].[desired_compression] ASC;

    OPEN [index_partition_cursor];

    DECLARE @curr_index_id          INT,
            @curr_data_compression  INT,
            @drop_current_index_ddl NVARCHAR(MAX),
            @drop_desired_index_ddl NVARCHAR(MAX);
    DECLARE @compress_current_ddl NVARCHAR(MAX),
            @compress_desired_ddl NVARCHAR(MAX);
    DECLARE @create_current_index_ddl NVARCHAR(MAX),
            @create_desired_index_ddl NVARCHAR(MAX);

    DECLARE @desired_compression_desc NVARCHAR(60);
    DECLARE @current_compression_desc NVARCHAR(60);
    DECLARE @v_control_current_index_creation INT;
    DECLARE @estimation_status NVARCHAR(4000) = N'''';

    FETCH NEXT FROM [index_partition_cursor]
    INTO @curr_index_id,
         @curr_data_compression,
         @curr_index_type,
         @curr_index_type_desc,
         @drop_current_index_ddl,
         @drop_desired_index_ddl,
         @create_current_index_ddl,
         @create_desired_index_ddl,
         @compress_current_ddl,
         @compress_desired_ddl,
         @index_name,
         @desired_compression,
         @v_control_current_index_creation;

    WHILE @@fetch_status = 0
    BEGIN
      DECLARE @current_size                  BIGINT,
              @sample_compressed_current     BIGINT,
              @sample_compressed_desired     BIGINT        = 0,
              @current_index_already_created BIT           = 0,
              @require_drop_current_index    BIT           = 0,
              @require_drop_desired_index    BIT           = 0,
              @require_drop_existing_on      BIT           = 0,
              @sample_index_id               INT,
              @current_heap_size_kb          NUMERIC(25, 2),
              @current_heap_used_pages       BIGINT;

      SET @estimation_status = NULL;

      SET @desired_compression_desc = CASE @desired_compression
                                        WHEN 0 THEN ''NONE''
                                        WHEN 1 THEN ''ROW''
                                        WHEN 2 THEN ''PAGE''
                                        WHEN 3 THEN ''COLUMNSTORE''
                                        WHEN 4 THEN ''COLUMNSTORE_ARCHIVE''
                                        WHEN 5 THEN ''COMPRESS''
                                      END;

      SET @current_compression_desc = CASE @curr_data_compression
                                        WHEN 0 THEN ''NONE''
                                        WHEN 1 THEN ''ROW''
                                        WHEN 2 THEN ''PAGE''
                                        WHEN 3 THEN ''COLUMNSTORE''
                                        WHEN 4 THEN ''COLUMNSTORE_ARCHIVE''
                                      END;

      SELECT @current_heap_size_kb = CONVERT(NUMERIC(25, 2), ([used_page_count] * 8)),
             @current_heap_used_pages = [used_page_count]
      FROM [tempdb].[sys].[dm_db_partition_stats]
      WHERE [object_id] = @sample_table_object_id
            AND [index_id] IN (0, 1);

      RAISERROR(
        ''------------------------------------------------------------------------------------------------------------------------------------------------'',
        0,
        0) WITH NOWAIT;
      SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Starting to apply ''
                           + @desired_compression_desc + '' compression on index '' + QUOTENAME(@index_name) + ''(''
                           + QUOTENAME(DB_NAME()) + ''.'' + QUOTENAME(@schema_name) + ''.'' + QUOTENAME(@object_name) + '')'';
      RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

      IF @v_control_current_index_creation = 1
      --AND ((@curr_index_id = 0 AND @current_compression_desc <> ''NONE'') OR (@curr_index_id <> 0))
      BEGIN
        BEGIN TRY
          IF @create_current_index_ddl IS NULL
          BEGIN
            SET @create_current_index_ddl = @create_desired_index_ddl;
          END;
          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                               + ''Creating current index on sample table, '' + ''create_current_index_ddl = ''
                               + @create_current_index_ddl;
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
          EXEC (@create_current_index_ddl);

          SET @sample_index_id = (SELECT TOP 1
                                         [index_id]
                                  FROM [tempdb].[sys].[indexes]
                                  WHERE [object_id] = @sample_table_object_id
                                  ORDER BY [index_id] DESC);

          -- Get sample''s size at current compression level
          SELECT @current_size_batch = CONVERT(NUMERIC(25, 2), ([used_page_count] * 8) / 1024.),
                 @sample_compressed_current = [used_page_count]
          FROM [tempdb].[sys].[dm_db_partition_stats]
          WHERE [object_id] = @sample_table_object_id
                AND [index_id] = @sample_index_id;
        END TRY
        BEGIN CATCH
          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                               + ''Error while trying to create current index on sample table. Skipping this index. ''
                               + ''create_current_index_ddl = '' + @create_current_index_ddl;
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
          SET @estimation_status = N''Error - Error while trying to create current index on sample table. Skipping this index. ''
                                   + N''create_current_index_ddl = '' + @create_current_index_ddl + CHAR(13) + CHAR(10)
                                   + N''; Error: '' + ERROR_MESSAGE();

          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error: '' + ERROR_MESSAGE();
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        END CATCH;
      END;

      -- Get Partition''s current size
      SET @current_size = (SELECT [used_page_count]
                           FROM [sys].[dm_db_partition_stats]
                           WHERE [object_id] = @object_id
                                 AND [index_id] = @curr_index_id
                                 AND [partition_number] = @curr_partition_number);

      IF (@curr_data_compression = @desired_compression)
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Desired compression is same as current compression, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Desired compression is same as current compression, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Checking for SQL Server limitations*/
      /*COMPRESS function is only available on SQL2016+*/
      IF @sqlmajorver < 13 /*SQL2016*/
         AND @desired_compression_desc = ''COMPRESS''
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''COMPRESS function is only available on SQL2016+, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - COMPRESS function is only available on SQL2016+, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*COLUMNSTORE_ARCHIVE is only available on SQL2014+*/
      IF @sqlmajorver < 12 /*SQL2014*/
         AND @create_desired_index_ddl LIKE ''%columnstore index%''
         AND @desired_compression_desc LIKE ''COLUMNSTORE_ARCHIVE%''
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''COLUMNSTORE_ARCHIVE compression is only available on SQL2014+, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - COLUMNSTORE_ARCHIVE compression is only available on SQL2014+, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Clustered ColumnStore index is only available on SQL2014+*/
      IF @sqlmajorver < 12 /*SQL2014*/
         AND @create_desired_index_ddl LIKE ''%create clustered columnstore index%''
         AND @desired_compression_desc LIKE ''COLUMNSTORE%''
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Clustered ColumnStore index is only available on SQL2014+, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Clustered ColumnStore index is only available on SQL2014+, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Nonclustered ColumnStore index is only available on SQL2012+*/
      IF @sqlmajorver <= 10 /*SQL2008*/
         AND @create_desired_index_ddl LIKE ''%create nonclustered columnstore index%''
         AND @desired_compression_desc LIKE ''COLUMNSTORE%''
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Nonclustered ColumnStore index is only available on SQL2012+, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Nonclustered ColumnStore index is only available on SQL2012+, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Filtered NonClustered Columnstore index is only available on SQL2016+*/
      IF @sqlmajorver <= 12 /*SQL2014*/
         AND @create_desired_index_ddl LIKE ''%create nonclustered columnstore index%''
         AND @create_desired_index_ddl LIKE ''% where %''
         AND @desired_compression_desc LIKE ''COLUMNSTORE%''
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Filtered Nonclustered ColumnStore index is only available on SQL2016+, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Filtered Nonclustered ColumnStore index is only available on SQL2016+, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END

      /*Datatype limitations for ColumnStore*/
      IF @create_desired_index_ddl LIKE ''%create clustered columnstore index%''
         AND @desired_compression_desc LIKE ''COLUMNSTORE%''
         AND EXISTS (SELECT *
                     FROM [sys].[columns]
                     WHERE [object_id] = @object_id
                           AND (
                               /*SQL2016- limitations for Clustered ColumnStore*/
                               (@sqlmajorver <= 13 /*SQL2016*/
                                AND [max_length] = -1
                                AND [system_type_id] IN (231 /*nvarchar*/, 167 /*varchar*/, 165 /*varbinary*/, 36 /*uniqueidentifier*/))
                               OR
                               /*SQL2017- limitations for Clustered ColumnStore*/
                               (@sqlmajorver < 14 /*SQL2017*/
                                AND [system_type_id] IN (36 /*uniqueidentifier*/))
                               OR
                               /*Limitations for Clustered ColumnStore, all versions*/
                               ([system_type_id] IN (99 /*ntext*/, 35 /*text*/, 34,                         /*image*/
                                                     189 /*timestamp and rowversion*/, 98,                  /*sql_variant*/
                                                     240 /*CLR types (hierarchyid and spatial types)*/, 241 /*XML*/))))
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Table has columns using unsupported data type for ColumnStore, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Table has columns using unsupported data type for ColumnStore, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Columnstore does not support persisted computed columns*/
      /*For clustered Columnstore, checking all table columns*/
      IF @create_desired_index_ddl LIKE ''%create clustered columnstore index%''
         AND @desired_compression_desc LIKE ''COLUMNSTORE%''
         AND EXISTS (SELECT 1
                     FROM [sys].[computed_columns] [c]
                     JOIN [sys].[tables] [t]
                     ON [t].[object_id] = [c].[object_id]
                     WHERE [c].[is_persisted] = 1
                           AND [t].[object_id] = @object_id)
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Table has persisted computed columns, columnstore index cannot include a computed column implicitly or explicitly, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Table has persisted computed columns, columnstore index cannot include a computed column implicitly or explicitly, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Columnstore does not support persisted computed columns*/
      /*For nonclustered Columnstore, checking index columns*/
      IF @create_desired_index_ddl LIKE ''%create nonclustered columnstore index%''
         AND @desired_compression_desc LIKE ''COLUMNSTORE%''
         AND EXISTS (SELECT 1
                     FROM [sys].[computed_columns] [c]
                     JOIN [sys].[index_columns] [ic]
                     ON [ic].[object_id] = [c].[object_id]
                        AND [ic].[column_id] = [c].[column_id]
                        AND [c].[is_persisted] = 1
                     WHERE [ic].[object_id] = @object_id
                           AND [ic].[index_id] = @curr_index_id)
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Index has persisted computed columns, columnstore index cannot include a computed column implicitly or explicitly, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @estimation_status = N''Skipped - Index has persisted computed columns, columnstore index cannot include a computed column implicitly or explicitly, skipping this test.'';
        SET @sample_compressed_desired = 0;
        GOTO MOVENEXT;
      END;

      /*Skip COMPRESS test for indexes diff than HEAP and Clustered*/
      IF @desired_compression_desc = ''COMPRESS''
         AND @curr_index_type NOT IN (0 /*HEAP*/, 1 /*Clustered rowstore*/)
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                             + ''Compress only applies for HEAP and Clustered indexes, skipping this test.'';
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        SET @sample_compressed_desired = 0;
        SET @estimation_status = N''Skipped - Compress only applies for HEAP and Clustered indexes, skipping this test.'';
        GOTO MOVENEXT;
      END;

      /*COMPRESS only applies for Clustered rowstore or Heap*/
      IF @desired_compression_desc = ''COMPRESS''
         AND @curr_index_type IN (0 /*HEAP*/, 1 /*Clustered rowstore*/)
      BEGIN
        /*Use heap table sample sizes as base for current space usage*/
        SET @sample_compressed_current = @current_heap_used_pages;

        BEGIN TRY
          SELECT @alter_ddl = [alter_ddl],
                 @insert_ddl = [insert_ddl]
          FROM [dbo].index_review_generate_table_sample_ddl(
                 @object_id,
                 @schema_name,
                 @object_name,
                 @curr_partition_number,
                 @curr_partition_column_id,
                 @curr_partition_function_id,
                 ''#sample_tableDBA05385A6FF40F888204D05C7D56D2B_compress'',
                 @dummy_column,
                 @requires_computed,
                 @sample_percent,
                 @desired_compression,
                 @compress_column_size);

          IF @insert_ddl NOT LIKE ''%compress(%''
          BEGIN
            SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                 + ''Table doesn''''t have columns elegible for COMPRESS, skipping this test.'';
            RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
            SET @estimation_status = N''Skipped - Table doesn''''t have columns elegible for COMPRESS, skipping this test.'';
            SET @sample_compressed_desired = 0;
            GOTO MOVENEXT;
          END;

          CREATE TABLE [#sample_tableDBA05385A6FF40F888204D05C7D56D2B_compress]
          (
            [dummyDBA05385A6FF40F888204D05C7D56D2B] INT
          );

          -- Add columns into sample table
          EXEC (@alter_ddl);
          ALTER TABLE [#sample_tableDBA05385A6FF40F888204D05C7D56D2B_compress]
          REBUILD;

          /* Changing source table to use existing sample table to make sure we''re runninng the test with the same data to get better accuracy*/
          SET @insert_ddl = REPLACE(
                              @insert_ddl,
                              QUOTENAME(@schema_name) + ''.'' + QUOTENAME(@object_name),
                              ''#sample_tableDBA05385A6FF40F888204D05C7D56D2B'');

          /* Removing tablesample hint */
          SET @insert_ddl = REPLACE(
                              @insert_ddl, ''tablesample ('' + CONVERT(VARCHAR(30), @sample_percent) + '' percent)'', '''');

          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                               + ''Populating table sample using compress function, '' + ''insert_ddl = '' + @insert_ddl;
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

          EXEC (@insert_ddl);

          /*Get object_id of compress table sample*/
          DECLARE @sample_table_object_id_compress INT = OBJECT_ID(
                                                           ''tempdb.dbo.#sample_tableDBA05385A6FF40F888204D05C7D56D2B_compress'');

          SELECT @current_size_batch = CONVERT(NUMERIC(25, 2), ([used_page_count] * 8) / 1024.)
          FROM [tempdb].[sys].[dm_db_partition_stats]
          WHERE [object_id] = @sample_table_object_id_compress;

          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                               + ''Finished to populate table sample using compress function, current table size = ''
                               + CONVERT(VARCHAR(30), @current_size_batch) + ''mb.'';
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        END TRY
        BEGIN CATCH
          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                               + ''Error while trying to apply COMPRESS on sample table. Skipping this index. ''
                               + ''insert_ddl = '' + @insert_ddl;
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
          SET @estimation_status = N''Error - Error while trying to apply COMPRESS on sample table. Skipping this index. ''
                                   + N''insert_ddl = '' + @insert_ddl + CHAR(13) + CHAR(10) + N''; Error: ''
                                   + ERROR_MESSAGE();

          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error: '' + ERROR_MESSAGE();
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
        END CATCH;

        SELECT @sample_compressed_desired = [used_page_count]
        FROM [tempdb].[sys].[dm_db_partition_stats]
        WHERE [object_id] = @sample_table_object_id_compress;

        GOTO MOVENEXT;
      END;

      IF (@create_desired_index_ddl IS NOT NULL)
      BEGIN
        BEGIN TRY

          IF @create_desired_index_ddl NOT LIKE ''alter table %''
          BEGIN
            SET @create_desired_index_ddl = REPLACE(
                                              @create_desired_index_ddl,
                                              ''with (data_compression = '',
                                              ''with (drop_existing=on, data_compression = '');
          END;

          /*Specify (data_compression = columnstore) on create index is only available on SQL2014+*/
          /*Conversion between columnstore index and relational index is only available on SQL2014+*/
          IF @sqlmajorver <= 11 /*SQL2012*/
             AND @create_desired_index_ddl LIKE ''%create nonclustered columnstore index%''
             AND @desired_compression_desc LIKE ''COLUMNSTORE%''
          BEGIN
            SET @create_desired_index_ddl = REPLACE(@create_desired_index_ddl, ''with (drop_existing=on, data_compression = columnstore)'', '''')

            /*Since conversion from relational to columnstore is not available, drop any existing index before continue*/
            BEGIN TRY
              SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                   + ''Running command to drop desired index on sample table. ''
                                   + ''drop_desired_index_ddl = '' + @drop_desired_index_ddl;
              RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

              EXEC (@drop_desired_index_ddl);
            END TRY
            BEGIN CATCH
              SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                   + ''Error while trying to drop desired index on sample table. ''
                                   + ''drop_desired_index_ddl = '' + @drop_desired_index_ddl;
              RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
              SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error: '' + ERROR_MESSAGE();
              RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
            END CATCH;
          END;

          SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                               + ''Creating desired index on sample table, '' + ''create_desired_index_ddl = ''
                               + @create_desired_index_ddl;
          RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

          EXEC (@create_desired_index_ddl);

          SET @sample_index_id = (SELECT TOP 1
                                         [index_id]
                                  FROM [tempdb].[sys].[indexes]
                                  WHERE [object_id] = @sample_table_object_id
                                  ORDER BY [index_id] DESC);

          -- Get sample''s size at desired compression level
          SELECT @sample_compressed_desired = [used_page_count]
          FROM [tempdb].[sys].[dm_db_partition_stats]
          WHERE [object_id] = @sample_table_object_id
                AND [index_id] = @sample_index_id;

        END TRY
        BEGIN CATCH
          IF ERROR_NUMBER() = 7999 /*Could not find any index named...*/
          BEGIN
            BEGIN TRY
              SET @create_desired_index_ddl = REPLACE(@create_desired_index_ddl, ''drop_existing=on, '', '''');

              SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                   + ''Index doesn''''t exist, re-executing command without drop_existing=on, ''
                                   + ''create_desired_index_ddl = '' + @create_desired_index_ddl;
              RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

              EXEC (@create_desired_index_ddl);

              SET @sample_index_id = (SELECT TOP 1
                                             [index_id]
                                      FROM [tempdb].[sys].[indexes]
                                      WHERE [object_id] = @sample_table_object_id
                                      ORDER BY [index_id] DESC);

              -- Get sample''s size at desired compression level
              SELECT @sample_compressed_desired = [used_page_count]
              FROM [tempdb].[sys].[dm_db_partition_stats]
              WHERE [object_id] = @sample_table_object_id
                    AND [index_id] = @sample_index_id;
            END TRY
            BEGIN CATCH
              SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                   + ''Error while trying to re-execute command to create desired compression index on sample table. Skipping this index. ''
                                   + ''create_desired_index_ddl = '' + @create_desired_index_ddl;
              RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
              SET @estimation_status = +N''Error - Error while trying to re-execute command to create desired compression index on sample table. Skipping this index. ''
                                       + N''create_desired_index_ddl = '' + @create_desired_index_ddl + CHAR(13)
                                       + CHAR(10) + N''; Error: '' + ERROR_MESSAGE();;
              SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error: '' + ERROR_MESSAGE();
              RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
            END CATCH;
          END;
          ELSE
          BEGIN
            SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                 + ''Error while trying to create desired compression index on sample table. Skipping this index. ''
                                 + ''create_desired_index_ddl = '' + @create_desired_index_ddl;
            RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

            SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error: '' + ERROR_MESSAGE();
            RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
          END;
        END CATCH;

        IF (SELECT TOP 1
                   [type]
            FROM [tempdb].[sys].[indexes]
            WHERE [object_id] = @sample_table_object_id
            ORDER BY [index_id] DESC) IN (5, 6)
           AND @desired_compression_desc = ''COLUMNSTORE_ARCHIVE''
        BEGIN
          SET @require_drop_existing_on = 1;
        END;
        ELSE IF (SELECT TOP 1
                        [type]
                 FROM [tempdb].[sys].[indexes]
                 WHERE [object_id] = @sample_table_object_id
                 ORDER BY [index_id] DESC) IN (5, 6)
                AND NOT EXISTS (SELECT *
                                FROM #tmp_data_compression
                                WHERE col_data_compression = ''COLUMNSTORE_ARCHIVE'')
        BEGIN
          SET @require_drop_existing_on = 1;
        END;
        ELSE
        BEGIN
          SET @require_drop_existing_on = 0;
        END;

        IF (@require_drop_existing_on = 1)
        BEGIN
          BEGIN TRY
            SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                 + ''Running command to drop desired index on sample table. ''
                                 + ''drop_desired_index_ddl = '' + @drop_desired_index_ddl;
            RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

            EXEC (@drop_desired_index_ddl);
            SET @require_drop_desired_index = 0;
          END TRY
          BEGIN CATCH
            SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                                 + ''Error while trying to drop desired index on sample table. Skipping this index. ''
                                 + ''drop_desired_index_ddl = '' + @drop_desired_index_ddl;
            RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
            SET @estimation_status = N''Error - Error while trying to drop desired index on sample table. Skipping this index. ''
                                     + N''drop_desired_index_ddl = '' + @drop_desired_index_ddl + CHAR(13) + CHAR(10)
                                     + N''; Error: '' + ERROR_MESSAGE();;

            SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error: '' + ERROR_MESSAGE();
            RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
          END CATCH;
        END;
      END;

      MOVENEXT:
      IF (@curr_data_compression = @desired_compression)
      BEGIN
        SET @sample_compressed_desired = @sample_compressed_current;
      END;

      DECLARE @estimated_compressed_size BIGINT = CASE @sample_compressed_current
                                                    WHEN 0 THEN 0
                                                    ELSE
                                                      @current_size
                                                      * ((1. * CAST(@sample_compressed_desired AS FLOAT))
                                                         / @sample_compressed_current)
                                                  END;

      SET @estimated_compressed_size = CASE
                                         WHEN @estimated_compressed_size = 0 THEN NULL
                                         ELSE @estimated_compressed_size
                                       END;
      SET @sample_compressed_desired = CASE
                                         WHEN @sample_compressed_desired = 0 THEN NULL
                                         ELSE @sample_compressed_desired
                                       END;

      IF @estimated_compressed_size IS NOT NULL
         AND (@estimation_status IS NULL
              OR @estimation_status = '''')
      BEGIN
        SET @estimation_status = N''OK - Results: Ratio = ''
                                 + ISNULL(
                                     CONVERT(
                                       VARCHAR(30),
                                       CONVERT(
                                         BIGINT,
                                         100 - ((@estimated_compressed_size * 8) * 100.0 / ((@current_size * 8))))),
                                     ''NA'') + N'', Current size = ''
                                 + ISNULL(CONVERT(VARCHAR(30), @sample_compressed_current * 8) + ''kb'', ''NA'')
                                 + N'', Compressed size = ''
                                 + ISNULL(CONVERT(VARCHAR(30), @sample_compressed_desired * 8) + ''kb'', ''NA'');
      END;

      INSERT INTO [#estimated_results]
      (
        [database_name],
        [object_name],
        [schema_name],
        [index_id],
        [index_name],
        [index_type_desc],
        [partition_number],
        [estimation_status],
        [current_data_compression],
        [estimated_data_compression],
        [compression_ratio],
        [row_count],
        [size_with_current_compression_setting(GB)],
        [size_with_requested_compression_setting(GB)],
        [size_compression_saving(GB)],
        [size_with_current_compression_setting(KB)],
        [size_with_requested_compression_setting(KB)],
        [sample_size_with_current_compression_setting(KB)],
        [sample_size_with_requested_compression_setting(KB)],
        [sample_compressed_page_count],
        [sample_pages_with_current_compression_setting],
        [sample_pages_with_requested_compression_setting]
      )
      VALUES
      (
        DB_NAME(),                                                                                          -- database_name - sysname
        @object_name,                                                                                       -- object_name - sysname
        @schema_name,                                                                                       -- schema_name - sysname
        @curr_index_id,                                                                                     -- index_id - int
        @index_name,                                                                                        -- index_name - sysname
        @curr_index_type_desc,                                                                              -- index_type_desc - NVARCHAR(60)
        @curr_partition_number,                                                                             -- partition_number - int
        @estimation_status,                                                                                 -- estimation_status - nvarchar(4000)
        @current_compression_desc,                                                                          -- current_data_compression - nvarchar(60)
        @desired_compression_desc,                                                                          -- estimated_data_compression - nvarchar(60)
        CONVERT(NUMERIC(25, 2), 100 - ((@estimated_compressed_size * 8) * 100.0 / ((@current_size * 8)))),  -- compression_ratio - numeric(25, 2)
        @row_count,                                                                                         -- row_count - bigint
        (@current_size * 8) / 1024. / 1024.,                                                                -- size_with_current_compression_setting(GB) - numeric(25, 2)
        (@estimated_compressed_size * 8) / 1024. / 1024.,                                                   -- size_with_requested_compression_setting(GB) - numeric(25, 2)
        ((@current_size * 8) / 1024. / 1024.) - ((@estimated_compressed_size * 8) / 1024. / 1024.),         -- size_compression_saving(GB) - numeric(25, 2)
        @current_size * 8,                                                                                  -- size_with_current_compression_setting(KB) - bigint
        @estimated_compressed_size * 8,                                                                     -- size_with_requested_compression_setting(KB) - bigint
        @sample_compressed_current * 8,                                                                     -- sample_size_with_current_compression_setting(KB) - bigint
        @sample_compressed_desired * 8,                                                                     -- sample_size_with_requested_compression_setting(KB) - bigint
        @sample_compressed_desired,                                                                         -- sample_compressed_page_count bigint
        @current_size,                                                                                      -- sample_pages_with_current_compression_setting - bigint
        @estimated_compressed_size                                                                          -- sample_pages_with_requested_compression_setting - bigint
      );

      IF @estimated_compressed_size IS NOT NULL
      BEGIN
        SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Results: Ratio = ''
                             + ISNULL(
                                 CONVERT(
                                   VARCHAR(30),
                                   CONVERT(
                                     BIGINT, 100 - ((@estimated_compressed_size * 8) * 100.0 / ((@current_size * 8))))),
                                 ''NA'') + '', Current size = ''
                             + ISNULL(CONVERT(VARCHAR(30), @sample_compressed_current * 8) + ''kb'', ''NA'')
                             + '', Compressed size = ''
                             + ISNULL(CONVERT(VARCHAR(30), @sample_compressed_desired * 8) + ''kb'', ''NA'');
        RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
      END;


      SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Finished to apply ''
                           + @desired_compression_desc + '' compression on index '' + QUOTENAME(@index_name) + ''(''
                           + QUOTENAME(DB_NAME()) + ''.'' + QUOTENAME(@schema_name) + ''.'' + QUOTENAME(@object_name) + '')'';
      RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

      FETCH NEXT FROM [index_partition_cursor]
      INTO @curr_index_id,
           @curr_data_compression,
           @curr_index_type,
           @curr_index_type_desc,
           @drop_current_index_ddl,
           @drop_desired_index_ddl,
           @create_current_index_ddl,
           @create_desired_index_ddl,
           @compress_current_ddl,
           @compress_desired_ddl,
           @index_name,
           @desired_compression,
           @v_control_current_index_creation;
    END;
    CLOSE [index_partition_cursor];
    DEALLOCATE [index_partition_cursor];

    --
    -- Step 4. Drop the sample table
    --
    DROP TABLE [#sample_tableDBA05385A6FF40F888204D05C7D56D2B];
    IF OBJECT_ID(''tempdb.dbo.#sample_tableDBA05385A6FF40F888204D05C7D56D2B_compress'') IS NOT NULL
      DROP TABLE [#sample_tableDBA05385A6FF40F888204D05C7D56D2B_compress];

    FETCH NEXT FROM [c]
    INTO @curr_partition_column_id,
         @curr_partition_function_id,
         @curr_partition_number,
         @requires_computed,
         @alter_ddl,
         @insert_ddl,
         @table_option_ddl,
         @sample_percent;
  END;
  CLOSE [c];
  DEALLOCATE [c];

  --
  -- drop xml schema collection
  --
  DECLARE [c] CURSOR LOCAL FAST_FORWARD FOR
  SELECT [drop_ddl]
  FROM [#xml_schema_ddl];
  OPEN [c];
  FETCH NEXT FROM [c]
  INTO @drop_ddl;
  WHILE @@fetch_status = 0
  BEGIN
    EXEC (@drop_ddl);

    FETCH NEXT FROM [c]
    INTO @drop_ddl;
  END;
  CLOSE [c];
  DEALLOCATE [c];

  IF OBJECT_ID(''tempdb.dbo.##tmpIndexCheck45_CompressionResult'') IS NOT NULL
  BEGIN
    INSERT INTO ##tmpIndexCheck45_CompressionResult
    SELECT *
    FROM [#estimated_results]
    WHERE estimated_data_compression IN (SELECT col_data_compression FROM #tmp_data_compression);
  END;
  ELSE
  BEGIN
    SELECT *
    FROM [#estimated_results]
    WHERE estimated_data_compression IN (SELECT col_data_compression FROM #tmp_data_compression)
    ORDER BY index_id ASC;
  END;

  DROP TABLE [#estimated_results];
  DROP TABLE [#xml_schema_ddl];

  RAISERROR(
    ''------------------------------------------------------------------------------------------------------------------------------------------------'',
    0,
    0) WITH NOWAIT;
  SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - ''
                       + ''Finished script execution, working on table '' + QUOTENAME(DB_NAME()) + ''.''
                       + QUOTENAME(@schema_name) + ''.'' + QUOTENAME(@object_name) + ''(RowCount = ''
                       + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, @row_count), 1), ''.00'', '''')
                       + '' | BaseTableSize = '' + CONVERT(VARCHAR(30), @table_size) + ''mb)'';
  RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
  RAISERROR(
    ''------------------------------------------------------------------------------------------------------------------------------------------------'',
    0,
    0) WITH NOWAIT;

  IF EXISTS (SELECT *
             FROM [INFORMATION_SCHEMA].[ROUTINES]
             WHERE [ROUTINE_NAME] = ''index_review_generate_type'')
    EXEC (''drop function dbo.index_review_generate_type'');
  IF EXISTS (SELECT *
             FROM [INFORMATION_SCHEMA].[ROUTINES]
             WHERE [ROUTINE_NAME] = ''index_review_column_definition'')
    EXEC (''drop function dbo.index_review_column_definition'');
  IF EXISTS (SELECT *
             FROM [INFORMATION_SCHEMA].[ROUTINES]
             WHERE [ROUTINE_NAME] = ''index_review_generate_table_sample_ddl'')
    EXEC (''drop function dbo.index_review_generate_table_sample_ddl'');
  IF EXISTS (SELECT *
             FROM [INFORMATION_SCHEMA].[ROUTINES]
             WHERE [ROUTINE_NAME] = ''index_review_generate_index_ddl'')
    EXEC (''drop function dbo.index_review_generate_index_ddl'');
END;
  '
  
  BEGIN TRY
    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to create create sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + ']'
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

    EXECUTE @sp_executesql @stmt = @SQL

    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to create create sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + ']'
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);
  END TRY
  BEGIN CATCH
    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error while trying to create sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + '] '
                      + 'ErrMsg = ' + ERROR_MESSAGE();
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

    FETCH NEXT FROM c_databases
    INTO @Database_Name
    CONTINUE;
  END CATCH;

  SET @SQL = 
  '
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*1 seconds*/

IF OBJECT_ID (''dbo.tmpIndexCheckMonitoring_Log'') IS NULL
BEGIN
  CREATE TABLE dbo.tmpIndexCheckMonitoring_Log (RowID INT IDENTITY(1, 1) PRIMARY KEY, Dt DATETIME DEFAULT GETDATE(), cStatus VARCHAR(MAX))
END

DECLARE @schema_name sysname, @object_name sysname, @row_count BIGINT, @i INT = 0, @table_count INT,
        @min_row_count BIGINT = ' + CONVERT(VARCHAR(30), @min_rows) + ', @min_mb BIGINT = ' + CONVERT(VARCHAR(30), @min_mb) + ', 
        @table_name sysname = '''', @status_msg VARCHAR(MAX) = '''', @cmd VARCHAR(MAX) = '''';

IF OBJECT_ID(''tempdb.dbo.#tmp_tables'') IS NOT NULL
  DROP TABLE #tmp_tables;

IF @table_name = '''' 
BEGIN
  SET @table_name = NULL
END

SELECT SCHEMA_NAME(tables.schema_id) AS schema_name,
       tables.name AS object_name,
       (SELECT SUM(rows) FROM sys.partitions WITH(nolock) WHERE partitions.object_id = tables.object_id AND partitions.index_id <= 1) AS row_count,
       tSize.reserved_size_mb
INTO #tmp_tables
FROM sys.tables WITH(nolock)
CROSS APPLY
      (
          SELECT CONVERT(DECIMAL(18, 2), SUM((st.reserved_page_count * 8) / 1024.)) reserved_size_mb
          FROM sys.dm_db_partition_stats AS st WITH(nolock)
          WHERE tables.object_id = st.object_id
      ) AS tSize
WHERE name = ISNULL(@table_name, name)

SELECT @table_count = COUNT(*)
FROM #tmp_tables
WHERE row_count >= @min_row_count /*Ignoring small tables*/
AND reserved_size_mb >= @min_mb /*Ignoring small tables*/

SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Number of tables to process: '' + CONVERT(VARCHAR(30), @table_count);
RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

DECLARE cCompress CURSOR FAST_FORWARD READ_ONLY FOR
SELECT schema_name,
       object_name,
       row_count
FROM #tmp_tables
WHERE row_count >= @min_row_count /*Ignoring small tables*/
AND reserved_size_mb >= @min_mb /*Ignoring small tables*/
ORDER BY row_count DESC;

OPEN cCompress;

FETCH cCompress
INTO @schema_name,
     @object_name,
     @row_count;

WHILE @@fetch_status = 0
BEGIN
  SET @i = @i + 1;

  BEGIN TRY
    RAISERROR(''------------------------------------------------------------------------------------------------------------------------------------------------'', 0, 0) WITH NOWAIT;
    SELECT @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + CONVERT(VARCHAR(30), @i) + '' of ''
                         + CONVERT(VARCHAR(30), @table_count) + '' - Working on '' + QUOTENAME(@schema_name) + ''.''
                         + QUOTENAME(@object_name) + ''(RowCount = ''
                         + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, @row_count), 1), ''.00'', '''') + '')'';
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);

    SET @cmd = ''EXEC dbo.sp_estimate_data_compression_savings_v2
                       @schema_name = '''''' + @schema_name + '''''',
                       @object_name = '''''' + @object_name + '''''',
                       @data_compression = N''''' + @desired_compression + ''''',
                       @max_mb_to_sample = ' + CONVERT(NVARCHAR(30), @max_mb_to_sample) + ', @batch_sample_size_mb = 5, @compress_column_size = ' + CONVERT(NVARCHAR(30), @compress_column_size) + ''';

    EXEC (@cmd);
  END TRY
  BEGIN CATCH
    SET @status_msg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error processing ObjectName = ''
                      + @object_name + '' - skipping this object. ErrMsg = '' + ERROR_MESSAGE();
    RAISERROR(@status_msg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@status_msg);
  END CATCH;

  FETCH cCompress
  INTO @schema_name,
       @object_name,
       @row_count;
END;

CLOSE cCompress;

DEALLOCATE cCompress;
  '

  BEGIN TRY
    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to run sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + ']'
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

    EXECUTE @sp_executesql @stmt = @SQL

    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to run sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + ']'
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);
  END TRY
  BEGIN CATCH
    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error while trying to run sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + '] '
                      + 'ErrMsg = ' + ERROR_MESSAGE();
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

    FETCH NEXT FROM c_databases
    INTO @Database_Name
    CONTINUE;
  END CATCH;

  SET @SQL = 
  '
    IF EXISTS (SELECT *
               FROM [INFORMATION_SCHEMA].[ROUTINES]
               WHERE [ROUTINE_NAME] = ''sp_estimate_data_compression_savings_v2'')
      EXEC (''DROP PROC dbo.sp_estimate_data_compression_savings_v2'');
  '

  BEGIN TRY
    EXECUTE @sp_executesql @stmt = @SQL
  END TRY
  BEGIN CATCH
    SET @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error while trying to remove sp_estimate_data_compression_savings_v2 on DB [' + @Database_Name + '] '
                      + 'ErrMsg = ' + ERROR_MESSAGE();
    RAISERROR(@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

    FETCH NEXT FROM c_databases
    INTO @Database_Name
    CONTINUE;
  END CATCH;

  FETCH NEXT FROM c_databases
  INTO @Database_Name
END
CLOSE c_databases
DEALLOCATE c_databases

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to run script.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);

DECLARE @rebuild_options VARCHAR(MAX) = ''

IF @online_rebuild = 1 
  SET @rebuild_options = @rebuild_options + ', ONLINE = ON'
IF @sort_in_tempdb = 1  
  SET @rebuild_options = @rebuild_options + ', SORT_IN_TEMPDB = ON'
IF @max_dop IS NOT NULL 
  SET @rebuild_options = @rebuild_options + ', MAXDOP = ' + CONVERT(VARCHAR, @max_dop)

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

SELECT 'Check 45 - Estimate compression savings' AS Info,
       t1.database_name,
       t1.object_name,
       t1.schema_name,
       t1.index_id,
       t1.index_name,
       t1.index_type_desc,
       t1.partition_number,
       t1.estimation_status,
       t1.current_data_compression,
       t1.estimated_data_compression,
       t1.compression_ratio,
       t1.[size_with_current_compression_setting(GB)],
       t1.[size_with_requested_compression_setting(GB)],
       t1.[size_compression_saving(GB)],
       t1.row_count AS current_number_of_rows_table,
       CONVERT(BIGINT, t1.row_count / t1.sample_compressed_page_count) AS avg_rows_per_page,
       ISNULL(a.user_seeks + a.user_scans + a.user_lookups + a.user_updates, 0) AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       ISNULL(tab3.avg_of_access_per_minute_based_on_index_usage_dmv, 0) AS avg_of_access_per_minute_based_on_index_usage_dmv,
       ISNULL(a.user_seeks, 0) AS user_seeks,
       ISNULL(a.user_scans, 0) AS user_scans,
       ISNULL(Tab6.Scans_Ratio, 0) AS user_scans_ratio,
       ISNULL(a.user_lookups, 0) AS user_lookups,
       ISNULL(a.user_updates, 0) AS user_updates,
       ISNULL(Number_of_Reads,0) AS number_of_reads, 
       ISNULL(Tab1.[Reads_Ratio], 0) AS reads_ratio,
       ISNULL([Total Writes], 0) AS number_of_writes,
       ISNULL(Tab2.[Writes_Ratio], 0) AS writes_ratio,
       ISNULL(a.singleton_lookup_count, 0) AS singleton_lookup_count,
       ISNULL(Tab4.singleton_lookup_ratio, 0) AS singleton_lookup_ratio,
       ISNULL(a.range_scan_count, 0) AS range_scan_count,
       ISNULL(Tab5.range_scan_ratio, 0) AS range_scan_ratio,
       a.page_io_latch_wait_count,
       CAST(1. * a.page_io_latch_wait_in_ms / NULLIF(a.page_io_latch_wait_count ,0) AS DECIMAL(12,2)) AS page_io_latch_avg_wait_ms,
       a.page_io_latch_wait_in_ms AS total_page_io_latch_wait_in_ms,
       CONVERT(VARCHAR(200), ((page_io_latch_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((page_io_latch_wait_in_ms) / 1000), 0), 108) AS total_page_io_latch_wait_d_h_m_s,
       a.last_datetime_obj_was_used,
       ISNULL(a.plan_cache_reference_count, 0) AS plan_cache_reference_count,
       a.fill_factor,
       ISNULL(a.Buffer_Pool_SpaceUsed_MB, 0) AS buffer_pool_spaceused_mb,
       ISNULL(a.Buffer_Pool_FreeSpace_MB, 0) AS buffer_pool_freespace_mb,
       CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / CASE WHEN a.Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE a.Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent,
       CONVERT(NUMERIC(25, 2), (a.in_row_reserved_page_count * 8) / 1024.) AS in_row_reserved_mb,
       CONVERT(NUMERIC(25, 2), (a.lob_reserved_page_count * 8) / 1024.) AS lob_reserved_mb,
       CONVERT(NUMERIC(25, 2), (a.row_overflow_reserved_page_count * 8) / 1024.) AS row_overflow_reserved_mb,
       a.large_value_types_out_of_row,
       t1.[size_with_current_compression_setting(KB)],
       t1.[size_with_requested_compression_setting(KB)],
       t1.[sample_size_with_current_compression_setting(KB)],
       t1.[sample_size_with_requested_compression_setting(KB)],
       t1.[sample_compressed_page_count],
       t1.sample_pages_with_current_compression_setting,
       t1.sample_pages_with_requested_compression_setting,
       'SET NOCOUNT ON; ' + 
       CASE index_type_desc 
         WHEN 'HEAP' THEN
           CASE 
             WHEN estimated_data_compression IN ('ROW', 'PAGE', 'NONE')
               THEN 
                 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET LOCK_TIMEOUT 120000;' + NCHAR(13) + NCHAR(10) +
                 'DECLARE @statusMsg VARCHAR(MAX) = ''''' + NCHAR(13) + NCHAR(10) +
                 'DECLARE @TableSize NUMERIC(25,4)' + NCHAR(13) + NCHAR(10) +
                 'BEGIN TRY' + NCHAR(13) + NCHAR(10) +
                 '  IF NOT EXISTS(SELECT * FROM ' + QUOTENAME(t1.[database_name]) + '.sys.partitions WHERE partitions.object_id = ' + CONVERT(VARCHAR(200), a.Object_ID) + ' AND index_id = ' + CONVERT(VARCHAR(200), t1.index_id) + ' AND partition_number = ' + CONVERT(VARCHAR(200), t1.partition_number) + ' AND data_compression_desc = ''' + CONVERT(VARCHAR(200), t1.estimated_data_compression) + ''')' + NCHAR(13) + NCHAR(10) + 
                 '  BEGIN' + NCHAR(13) + NCHAR(10) +
                 '    SELECT @TableSize = CONVERT(NUMERIC(25,4), SUM(reserved_page_count * 8) / 1024. / 1024.) FROM ' + QUOTENAME(t1.[database_name]) + '.sys.dm_db_partition_stats WHERE object_id = ' + CONVERT(VARCHAR(200), a.Object_ID) + ' AND index_id = ' + CONVERT(VARCHAR(200), t1.index_id) + ' AND partition_number = ' + CONVERT(VARCHAR(200), t1.partition_number) + '' + NCHAR(13) + NCHAR(10) +
                 '    SELECT @statusMsg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Starting to apply ' + t1.estimated_data_compression + ' compression on obj ' + ISNULL(QUOTENAME(t1.[index_name]),'ALL') + ' on ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ', Current table size = '' + CONVERT(VARCHAR(50), @TableSize) + ''mb''' + NCHAR(13) + NCHAR(10) +
                 '    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);' + NCHAR(13) + NCHAR(10) +
                 '    ALTER TABLE ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ' REBUILD PARTITION = ' + CASE WHEN a.IsIndexPartitioned = 1 THEN CONVERT(VARCHAR(30), t1.partition_number) ELSE 'ALL' END + ' WITH(DATA_COMPRESSION=' + t1.estimated_data_compression + @rebuild_options + ')' + NCHAR(13) + NCHAR(10) + 
                 '    SELECT @TableSize = CONVERT(NUMERIC(25,4), SUM(reserved_page_count * 8) / 1024. / 1024.) FROM ' + QUOTENAME(t1.[database_name]) + '.sys.dm_db_partition_stats WHERE object_id = ' + CONVERT(VARCHAR(200), a.Object_ID) + ' AND index_id = ' + CONVERT(VARCHAR(200), t1.index_id) + ' AND partition_number = ' + CONVERT(VARCHAR(200), t1.partition_number) + '' + NCHAR(13) + NCHAR(10) +
                 '    SELECT @statusMsg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Finished to apply ' + t1.estimated_data_compression + ' compression on obj ' + ISNULL(QUOTENAME(t1.[index_name]),'ALL') + ' on ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ', Current table size = '' + CONVERT(VARCHAR(50), @TableSize) + ''mb''' + NCHAR(13) + NCHAR(10) +
                 '    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg); ' + NCHAR(13) + NCHAR(10) +
                 '  END; ' + NCHAR(13) + NCHAR(10) +
                 'END TRY' + NCHAR(13) + NCHAR(10) +
                 'BEGIN CATCH' + NCHAR(13) + NCHAR(10) +
                 '  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error trying to run rebuild.''' + NCHAR(13) + NCHAR(10) +
                 '  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);' + NCHAR(13) + NCHAR(10) +
                 '  SELECT @statusMsg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Error_Message: '' + ERROR_MESSAGE()' + NCHAR(13) + NCHAR(10) +
                 '  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg); ' + NCHAR(13) + NCHAR(10) +
                 'END CATCH' + NCHAR(13) + NCHAR(10) +
                 'GO' + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10)
             ELSE ''
           END
         ELSE 
           CASE 
             WHEN estimated_data_compression IN ('ROW', 'PAGE', 'NONE')
               THEN 
                 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET LOCK_TIMEOUT 120000;' + NCHAR(13) + NCHAR(10) +
                 'DECLARE @statusMsg VARCHAR(MAX) = ''''' + NCHAR(13) + NCHAR(10) +
                 'DECLARE @TableSize NUMERIC(25,4)' + NCHAR(13) + NCHAR(10) +
                 'BEGIN TRY' + NCHAR(13) + NCHAR(10) +
                 '  IF NOT EXISTS(SELECT * FROM ' + QUOTENAME(t1.[database_name]) + '.sys.partitions WHERE partitions.object_id = ' + CONVERT(VARCHAR(200), a.Object_ID) + ' AND index_id = ' + CONVERT(VARCHAR(200), t1.index_id) + ' AND partition_number = ' + CONVERT(VARCHAR(200), t1.partition_number) + ' AND data_compression_desc = ''' + CONVERT(VARCHAR(200), t1.estimated_data_compression) + ''')' + NCHAR(13) + NCHAR(10) + 
                 '  BEGIN' + NCHAR(13) + NCHAR(10) +
                 '    SELECT @TableSize = CONVERT(NUMERIC(25,4), SUM(reserved_page_count * 8) / 1024. / 1024.) FROM ' + QUOTENAME(t1.[database_name]) + '.sys.dm_db_partition_stats WHERE object_id = ' + CONVERT(VARCHAR(200), a.Object_ID) + ' AND index_id = ' + CONVERT(VARCHAR(200), t1.index_id) + ' AND partition_number = ' + CONVERT(VARCHAR(200), t1.partition_number) + '' + NCHAR(13) + NCHAR(10) +
                 '    SELECT @statusMsg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Starting to apply ' + t1.estimated_data_compression + ' compression on obj ' + ISNULL(QUOTENAME(t1.[index_name]),'ALL') + ' on ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ', Current table size = '' + CONVERT(VARCHAR(50), @TableSize) + ''mb''' + NCHAR(13) + NCHAR(10) +
                 '    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);' + NCHAR(13) + NCHAR(10) +
                 CASE 
                   WHEN a.IsIndexPartitioned = 1 
                     THEN '    ALTER INDEX ' + ISNULL(QUOTENAME(t1.[index_name]),'ALL') +' ON ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ' REBUILD PARTITION = ' + CASE WHEN a.IsIndexPartitioned = 1 THEN CONVERT(VARCHAR(30), t1.partition_number) ELSE 'ALL' END + ' WITH(DATA_COMPRESSION=' + t1.estimated_data_compression + @rebuild_options + ')' 
                   ELSE 
                          '    ALTER INDEX ' + ISNULL(QUOTENAME(t1.[index_name]),'ALL') +' ON ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ' REBUILD WITH(FILLFACTOR = 100, DATA_COMPRESSION=' + t1.estimated_data_compression + @rebuild_options + ')'
                 END + NCHAR(13) + NCHAR(10) +  
                 '    SELECT @TableSize = CONVERT(NUMERIC(25,4), SUM(reserved_page_count * 8) / 1024. / 1024.) FROM ' + QUOTENAME(t1.[database_name]) + '.sys.dm_db_partition_stats WHERE object_id = ' + CONVERT(VARCHAR(200), a.Object_ID) + ' AND index_id = ' + CONVERT(VARCHAR(200), t1.index_id) + ' AND partition_number = ' + CONVERT(VARCHAR(200), t1.partition_number) + '' + NCHAR(13) + NCHAR(10) +
                 '    SELECT @statusMsg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Finished to apply ' + t1.estimated_data_compression + ' compression on obj ' + ISNULL(QUOTENAME(t1.[index_name]),'ALL') + ' on ' + QUOTENAME(t1.[database_name]) + '.' + QUOTENAME(t1.[schema_name]) + '.' + QUOTENAME(t1.[object_name]) + ', Current table size = '' + CONVERT(VARCHAR(50), @TableSize) + ''mb''' + NCHAR(13) + NCHAR(10) +
                 '    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);' + NCHAR(13) + NCHAR(10) +
                 '  END; ' + NCHAR(13) + NCHAR(10) +
                 'END TRY' + NCHAR(13) + NCHAR(10) +
                 'BEGIN CATCH' + NCHAR(13) + NCHAR(10) +
                 '  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error trying to run rebuild.''' + NCHAR(13) + NCHAR(10) +
                 '  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);' + NCHAR(13) + NCHAR(10) +
                 '  SELECT @statusMsg = ''['' + CONVERT(NVARCHAR(200), GETDATE(), 120) + ''] - Error_Message: '' + ERROR_MESSAGE()' + NCHAR(13) + NCHAR(10) +
                 '  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT; INSERT INTO dbo.tmpIndexCheckMonitoring_Log(cStatus) VALUES(@statusMsg);' + NCHAR(13) + NCHAR(10) +
                 'END CATCH' + NCHAR(13) + NCHAR(10) +
                 'GO' + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10)
             ELSE ''
           END
       END AS SqlToCompress
INTO #tmp1
FROM ##tmpIndexCheck45_CompressionResult AS t1
LEFT OUTER JOIN dbo.Tab_GetIndexInfo a
ON a.Database_Name = t1.database_name
AND a.Schema_Name = t1.schema_name
AND a.Table_Name = t1.object_name
AND a.Index_ID = t1.index_id
OUTER APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN (a.user_seeks + a.user_scans + a.user_lookups) = 0 THEN 0 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups)) * 100 /
              		    CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)) END END AS DECIMAL(18,2))),0)) AS Tab1([Reads_Ratio])
OUTER APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN a.user_updates = 0 THEN 0 ELSE CONVERT(REAL, a.user_updates) * 100 /
		                  CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)) END END AS DECIMAL(18,2))),0)) AS Tab2([Writes_Ratio])
OUTER APPLY (SELECT CONVERT(VARCHAR(200), user_seeks + user_scans + user_lookups + user_updates / 
                             CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                               WHEN 0 THEN 1
                               ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                             END)) AS tab3(avg_of_access_per_minute_based_on_index_usage_dmv)
OUTER APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN (singleton_lookup_count) = 0 THEN 0 ELSE CONVERT(REAL, (singleton_lookup_count)) * 100 /
              		    CASE (singleton_lookup_count + range_scan_count) WHEN 0 THEN 1 ELSE CONVERT(REAL, (singleton_lookup_count + range_scan_count)) END END AS DECIMAL(18,2))),0)) AS Tab4(singleton_lookup_ratio)
OUTER APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN range_scan_count = 0 THEN 0 ELSE CONVERT(REAL, range_scan_count) * 100 /
		                  CASE (singleton_lookup_count + range_scan_count) WHEN 0 THEN 1 ELSE CONVERT(REAL, (singleton_lookup_count + range_scan_count)) END END AS DECIMAL(18,2))),0)) AS Tab5(range_scan_ratio)
OUTER APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN (a.user_scans) = 0 THEN 0 ELSE CONVERT(REAL, (a.user_scans)) * 100 /
              		    CASE (a.user_seeks + a.user_scans + a.user_lookups) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups)) END END AS DECIMAL(18,2))),0)) AS Tab6(Scans_Ratio)

CREATE CLUSTERED INDEX ix1 ON #tmp1(database_name, object_name, schema_name, index_name, partition_number)

SELECT 
#tmp1.*,
t1.none_compression_ratio,
t1.row_compression_ratio,
t1.page_compression_ratio,
t1.columstore_compression_ratio,
t1.columstore_archive_compression_ratio,
t1.compress_compression_ratio
INTO dbo.tmpIndexCheck45
FROM #tmp1
CROSS APPLY (
  SELECT SUM(CASE WHEN a.estimated_data_compression = 'NONE' THEN a.compression_ratio END) AS none_compression_ratio,
         SUM(CASE WHEN a.estimated_data_compression = 'ROW' THEN a.compression_ratio END) AS row_compression_ratio,
         SUM(CASE WHEN a.estimated_data_compression = 'PAGE' THEN a.compression_ratio END) AS page_compression_ratio,
         SUM(CASE WHEN a.estimated_data_compression = 'COLUMNSTORE' THEN a.compression_ratio END) AS columstore_compression_ratio,
         SUM(CASE WHEN a.estimated_data_compression = 'COLUMNSTORE_ARCHIVE' THEN a.compression_ratio END) AS columstore_archive_compression_ratio,
         SUM(CASE WHEN a.estimated_data_compression = 'COMPRESS' THEN a.compression_ratio END) AS compress_compression_ratio
  FROM #tmp1 AS a
  WHERE a.database_name = #tmp1.database_name
  AND a.schema_name = #tmp1.schema_name
  AND a.object_name = #tmp1.object_name
  AND a.index_name = #tmp1.index_name
  AND a.partition_number = #tmp1.partition_number
) AS t1


ALTER TABLE dbo.tmpIndexCheck45 ADD CompressionOrderWeight INT NOT NULL DEFAULT 0
ALTER TABLE dbo.tmpIndexCheck45 ADD CompressionOrder INT NOT NULL DEFAULT 0
ALTER TABLE dbo.tmpIndexCheck45 ADD IsRecommendedAlgorithm INT NOT NULL DEFAULT 0


/* Define initial order based on buffer pool usage, index size and compression ratio. */

;WITH CTE_1
AS
(
  SELECT CompressionOrder, 
         ROW_NUMBER() OVER(ORDER BY tmpIndexCheck45.database_name,
                                    tmpIndexCheck45.current_number_of_rows_table DESC,
                                    tmpIndexCheck45.schema_name,
                                    tmpIndexCheck45.object_name,
                                    tmpIndexCheck45.index_id,
                                    tmpIndexCheck45.partition_number,
                                    CASE tmpIndexCheck45.estimated_data_compression
                                      WHEN 'NONE' THEN 0
                                      WHEN 'ROW' THEN 1
                                      WHEN 'PAGE' THEN 2
                                      WHEN 'COLUMNSTORE' THEN 3
                                      WHEN 'COLUMNSTORE_ARCHIVE' THEN 4
                                      WHEN 'COMPRESS' THEN 5
                                    END) AS rn
FROM dbo.tmpIndexCheck45
)
UPDATE CTE_1 SET CompressionOrder = rn

/* If index is already compressed and 
   NONE compression ratio is >= -3 and <= 3%, 
   then set preferable compression algorithm to NONE,
   as it is probably better to leave it as NONE to avoid
   extra overhead for a little gain */

UPDATE dbo.tmpIndexCheck45 SET IsRecommendedAlgorithm = 1
WHERE current_data_compression <> 'NONE'
AND estimated_data_compression = 'NONE'
AND compression_ratio BETWEEN -3 AND 3
AND NOT EXISTS(SELECT * FROM dbo.tmpIndexCheck45 AS a 
               WHERE a.database_name = tmpIndexCheck45.database_name
               AND a.object_name = tmpIndexCheck45.object_name
               AND a.schema_name = tmpIndexCheck45.schema_name
               AND a.index_name = tmpIndexCheck45.index_name
               AND a.estimated_data_compression <> 'NONE'
               AND a.IsRecommendedAlgorithm = 1)

/* If index is already compressed with PAGE and 
   ROW compression ratio is >= -3 and <= 3%, 
   then set preferable compression algorithm to ROW,
   as it is probably better to leave it as ROW to avoid
   extra overhead for a little gain compared to PAGE */

UPDATE dbo.tmpIndexCheck45 SET IsRecommendedAlgorithm = 1
WHERE current_data_compression = 'PAGE'
AND estimated_data_compression = 'ROW'
AND compression_ratio BETWEEN -3 AND 3
AND NOT EXISTS(SELECT * FROM dbo.tmpIndexCheck45 AS a 
               WHERE a.database_name = tmpIndexCheck45.database_name
               AND a.object_name = tmpIndexCheck45.object_name
               AND a.schema_name = tmpIndexCheck45.schema_name
               AND a.index_name = tmpIndexCheck45.index_name
               AND a.estimated_data_compression <> 'ROW'
               AND a.IsRecommendedAlgorithm = 1)

/* If row compression ratio is >= 10% and ratio difference 
   from ROW to PAGE is less than 10%, then set preferable 
   compression algorithm to ROW */
DECLARE @DiffToPreferRow INT = 10
DECLARE @MinRowCompressionRatio INT = 10

UPDATE dbo.tmpIndexCheck45 SET IsRecommendedAlgorithm = 1
WHERE estimated_data_compression = 'ROW'
AND page_compression_ratio - row_compression_ratio <= @DiffToPreferRow
AND IsRecommendedAlgorithm = 0
AND compression_ratio >= @MinRowCompressionRatio
AND NOT EXISTS(SELECT * FROM dbo.tmpIndexCheck45 AS a 
               WHERE a.database_name = tmpIndexCheck45.database_name
               AND a.object_name = tmpIndexCheck45.object_name
               AND a.schema_name = tmpIndexCheck45.schema_name
               AND a.index_name = tmpIndexCheck45.index_name
               AND a.estimated_data_compression <> 'ROW'
               AND a.IsRecommendedAlgorithm = 1)

/* If page compression ratio is >= 25% then
   set preferable compression algorithm to PAGE */
DECLARE @MinPageCompressionRatio INT = 25

UPDATE dbo.tmpIndexCheck45 SET IsRecommendedAlgorithm = 1
WHERE estimated_data_compression = 'PAGE'
AND compression_ratio >= @MinPageCompressionRatio
AND NOT EXISTS(SELECT * FROM dbo.tmpIndexCheck45 AS a 
               WHERE a.database_name = tmpIndexCheck45.database_name
               AND a.object_name = tmpIndexCheck45.object_name
               AND a.schema_name = tmpIndexCheck45.schema_name
               AND a.index_name = tmpIndexCheck45.index_name
               AND a.estimated_data_compression <> 'PAGE'
               AND a.IsRecommendedAlgorithm = 1)

/* An index >= 5% of total buffer pool data cache size (since "big" can vary on each environment, I'm using BP as a measure to help answer this.) 
   is considered a big table, will gain a extra weight on final formula.
   The idea is to favor compression on big tables. */
DECLARE @PercentOfMemoryToIdentifyBigTables INT = 5

;WITH CTE_1
AS
(
SELECT * FROM dbo.tmpIndexCheck45
WHERE ([size_with_current_compression_setting(KB)] / 1024.) >= (SELECT (CONVERT(NUMERIC(18, 2), SUM(pages_kb) / 1024.)  * @PercentOfMemoryToIdentifyBigTables) / 100  AS PercentOfBpSizeMB
                                                                FROM sys.dm_os_memory_clerks
                                                                WHERE [type] = 'MEMORYCLERK_SQLBUFFERPOOL')
)
UPDATE CTE_1 SET CompressionOrderWeight = CompressionOrderWeight + 1

/* Indexes with a high percent (default of 40%) of reads compared to writes will get an extra weight on final formula.
   The idea is to favor compression on tables with a high number of reads compared to writes. */
DECLARE @HighPercentOfReads INT = 40

UPDATE dbo.tmpIndexCheck45 SET CompressionOrderWeight = CompressionOrderWeight + 1
WHERE reads_ratio >= @HighPercentOfReads


/* Indexes with a high percent (default of 40%) of writes compared to reads will get a -1 weight on final formula.
   The idea is to disfavor compression on tables with a high number of writes compared to reads. */
DECLARE @HighPercentOfWrites INT = 40
UPDATE dbo.tmpIndexCheck45 SET CompressionOrderWeight = CompressionOrderWeight - 1
WHERE writes_ratio >= @HighPercentOfWrites

/* Indexes with high waits (default of TOP 500 indexes by page IO latch) on page IO latch will get an extra weight on final formula.
   The idea is to favor compression on indexes waiting to be read from disk. */
DECLARE @TopNPageIOLatch INT = 500

IF OBJECT_ID('tempdb.dbo.#tmp_top_pageio_latch') IS NOT NULL
  DROP TABLE #tmp_top_pageio_latch

SELECT TOP (@TopNPageIOLatch) database_name, object_name, schema_name, index_name 
INTO #tmp_top_pageio_latch
FROM dbo.tmpIndexCheck45
ORDER BY page_io_latch_wait_count DESC

UPDATE dbo.tmpIndexCheck45 SET CompressionOrderWeight = CompressionOrderWeight + 1
WHERE EXISTS(SELECT * FROM #tmp_top_pageio_latch AS a 
             WHERE a.database_name = tmpIndexCheck45.database_name
             AND a.object_name = tmpIndexCheck45.object_name
             AND a.schema_name = tmpIndexCheck45.schema_name
             AND a.index_name = tmpIndexCheck45.index_name)

/* Indexes with a high (default of TOP 500 indexes by buffer pool memory usage) buffer pool memory usage will get an extra weight on final formula.
   The idea is to favor compression on indexes using lot of memory. */
/* If PLE is low (default of 500), double the @TopNBufferPool value to get more indexes compressed. */
DECLARE @LowPLEValue INT = 500
DECLARE @TopNBufferPool INT = 500

IF EXISTS(SELECT *
          FROM sys.dm_os_performance_counters
          WHERE OBJECT_NAME LIKE '%Buffer Node%'
          AND counter_name LIKE '%Page life%'
          AND cntr_value <= @LowPLEValue)
BEGIN
  SET @TopNBufferPool = @TopNBufferPool * 2
END

IF OBJECT_ID('tempdb.dbo.#tmp_top_bp') IS NOT NULL
  DROP TABLE #tmp_top_bp

SELECT TOP (@TopNBufferPool) database_name, object_name, schema_name, index_name 
INTO #tmp_top_bp
FROM dbo.tmpIndexCheck45
ORDER BY buffer_pool_spaceused_mb DESC

UPDATE dbo.tmpIndexCheck45 SET CompressionOrderWeight = CompressionOrderWeight + 1
WHERE EXISTS(SELECT * FROM #tmp_top_bp AS a 
             WHERE a.database_name = tmpIndexCheck45.database_name
             AND a.object_name = tmpIndexCheck45.object_name
             AND a.schema_name = tmpIndexCheck45.schema_name
             AND a.index_name = tmpIndexCheck45.index_name)

/* Most used (default of TOP 200 indexes by avg of access per minute) indexes will get an extra weight on final formula.
   The idea is to favor compression on most used indexes. */
DECLARE @TopNMostUsedIndexes INT = 200
IF OBJECT_ID('tempdb.dbo.#tmp_most_used') IS NOT NULL
  DROP TABLE #tmp_most_used

SELECT TOP (@TopNMostUsedIndexes) database_name, object_name, schema_name, index_name 
INTO #tmp_most_used
FROM dbo.tmpIndexCheck45
ORDER BY avg_of_access_per_minute_based_on_index_usage_dmv DESC

UPDATE dbo.tmpIndexCheck45 SET CompressionOrderWeight = CompressionOrderWeight + 1
WHERE EXISTS(SELECT * FROM #tmp_most_used AS a 
             WHERE a.database_name = tmpIndexCheck45.database_name
             AND a.object_name = tmpIndexCheck45.object_name
             AND a.schema_name = tmpIndexCheck45.schema_name
             AND a.index_name = tmpIndexCheck45.index_name)


/* Select final results */
SELECT Info,
       database_name,
       object_name,
       schema_name,
       index_id,
       index_name,
       index_type_desc,
       partition_number,
       estimation_status,
       current_data_compression,
       estimated_data_compression,
       compression_ratio,
       [size_with_current_compression_setting(GB)],
       [size_with_requested_compression_setting(GB)],
       [size_compression_saving(GB)],
       current_number_of_rows_table,
       avg_rows_per_page,
       number_of_access_on_index_table_since_last_restart_or_rebuild,
       avg_of_access_per_minute_based_on_index_usage_dmv,
       user_seeks,
       user_scans,
       user_scans_ratio,
       user_lookups,
       user_updates,
       number_of_reads,
       reads_ratio,
       number_of_writes,
       writes_ratio,
       singleton_lookup_count,
       singleton_lookup_ratio,
       range_scan_count,
       range_scan_ratio,
       page_io_latch_wait_count,
       page_io_latch_avg_wait_ms,
       total_page_io_latch_wait_in_ms,
       total_page_io_latch_wait_d_h_m_s,
       last_datetime_obj_was_used,
       plan_cache_reference_count,
       fill_factor,
       buffer_pool_spaceused_mb,
       buffer_pool_freespace_mb,
       Buffer_Pool_FreeSpace_Percent,
       in_row_reserved_mb,
       lob_reserved_mb,
       row_overflow_reserved_mb,
       large_value_types_out_of_row,
       [size_with_current_compression_setting(KB)],
       [size_with_requested_compression_setting(KB)],
       [sample_size_with_current_compression_setting(KB)],
       [sample_size_with_requested_compression_setting(KB)],
       sample_compressed_page_count,
       sample_pages_with_current_compression_setting,
       sample_pages_with_requested_compression_setting,
       none_compression_ratio,
       row_compression_ratio,
       page_compression_ratio,
       columstore_compression_ratio,
       columstore_archive_compression_ratio,
       compress_compression_ratio,
       CompressionOrderWeight,
       CompressionOrder,
       IsRecommendedAlgorithm,
       SqlToCompress
FROM dbo.tmpIndexCheck45
WHERE 1=1
ORDER BY CompressionOrderWeight DESC, CompressionOrder ASC

/*
-- Return final script 
SELECT SqlToCompress AS "text()"
FROM tempdb.dbo.tmpIndexCheck45
WHERE 1=1
AND IsRecommendedAlgorithm = 1
ORDER BY CompressionOrderWeight DESC, CompressionOrder ASC
FOR XML PATH('') 
*/
