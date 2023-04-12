/* 
Check11 - Key column size

Description:
The maximum number of bytes in a clustered index key can't exceed 900. For a nonclustered index key, the maximum is 1,700 bytes.
You can define a key using variable-length columns whose maximum sizes add up to more than the limit. However, the combined sizes of the data in those columns can never exceed the limit. 

Estimated Benefit:
Low

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review indexes with keys larger than the allowed size in bytes for the index.

Detailed recommendation:
When you design an index that contains many key columns, or large-size columns, calculate the size of the index key to make sure that you do not exceed the maximum index key size. This excludes nonkey columns that are included in the definition of nonclustered indexes.
Review indexes key columns to avoid errors.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck11') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck11

DECLARE @sqlmajorver INT
SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff);

SELECT 'Check 11 - Key column size' AS [Info],
        a.Database_Name,
        a.Schema_Name,
        a.Table_Name,
        a.Index_Name,
        a.Index_Type,
        a.indexed_columns,
        a.Number_Rows AS current_number_of_rows_table,
        a.ReservedSizeInMB,
        user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
        a.last_datetime_obj_was_used,
        a.[KeyCols_data_length_bytes],
        CASE
            WHEN @sqlmajorver < 13 THEN
                '[WARNING: Index key is larger than 900 bytes. It is recommended to revise these]'
            ELSE
                '[WARNING: Index key is larger than allowed (900 bytes for clustered index; 1700 bytes for nonclustered index). It is recommended to revise these]'
        END AS [Comment]
   INTO tempdb.dbo.tmpIndexCheck11
   FROM tempdb.dbo.Tab_GetIndexInfo AS a
  WHERE (
              [KeyCols_data_length_bytes] > 900
              AND @sqlmajorver < 13
          )
          OR
          (
              [KeyCols_data_length_bytes] > 900
              AND Index_Type IN ( 'CLUSTERED', 'CLUSTERED COLUMNSTORE' )
              AND @sqlmajorver >= 13
          )
          OR
          (
              [KeyCols_data_length_bytes] > 1700
              AND Index_Type IN ( 'NONCLUSTERED', 'NONCLUSTERED COLUMNSTORE' )
              AND @sqlmajorver >= 13
          )

SELECT * FROM tempdb.dbo.tmpIndexCheck11
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name