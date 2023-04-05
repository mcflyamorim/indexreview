/* 
Check15 – GUID as index key

Description:
A big reason for a clustered index is to retrieve rows for a range of values for a given column. Because the data is physically arranged in that order, the rows can be extracted very efficiently. Something like a GUID, while excellent for a primary key, could be positively detrimental to performance, as there will be additional cost for inserts and no perceptible benefit on selects.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Remove GUIDs in clustered indexes keys.

Detailed recommendation:
The choice of a “perfect clustered index key” depends of many factors (amount of memory available, DB size, number of non-clustered indexes, OLTP or OLAP workload, application orientation for writes vs reads, storage performance for random vs sequential I/Os, whether application queries are doing range or singleton reads using the key, just to list a few). 
Experienced database administrators can design a good set of indexes, but this task is complex, time-consuming, and error-prone even for moderately complex databases and workloads. Understanding the characteristics of the database, queries, and data columns can help customers to design optimal indexes.
Since there are many factors should be considered, in general, we recommend customers to use the following general guidelines for a cluster key, the idea is that by following the general guidelines you should cover the best practices for most of the cases, and then, fine tune the exceptions.
General guidelines for a good clustered index key:
•	Define the clustered index key with as few columns as possible. 
•	Keep the length of the index key short.
•	Consider columns that have one or more of the following attributes:
o	Are unique or contain many distinct values.
o	Are accessed sequentially.
o	Used frequently to sort the data retrieved from a table.
o	Defined as IDENTITY.
Typically, a cluster index is good candidate when you have queries doing the following:
•	Return a range of values by using operators such as BETWEEN, >, >=, <, and <=. After the row with the first value is found by using the clustered index, rows with subsequent indexed values are guaranteed to be physically adjacent. For example, if a query retrieves records between a range of sales order numbers, a clustered index on the column SalesOrderNumber can quickly locate the row that contains the starting sales order number, and then retrieve all successive rows in the table until the last sales order number is reached.
•	Return large result sets.
•	Reading many columns in a table.
•	Use JOIN clauses; typically, these are foreign key columns.
•	Use ORDER BY or GROUP BY clauses.
•	An index on the columns specified in the ORDER BY or GROUP BY clause may remove the need for the Database Engine to sort the data, because the rows are already sorted. This improves query performance.

  Note 1: There are several cases where a GUID as a key column is acceptable, therefore, customers should evaluate each case to confirm.
  Note 2: You can look at number of range or singleton reads and check if GUID is bad or really bad.
  Note 3: My general advice is just don’t use them as predicates, or on anything that requires good estimates for range scans. If you really, really have to use them as PK on a table to maintain uniqueness and therefore leverage singleton lookups as possible, then GUID is ok enough. But use NEWSEQUENTIALID generation instead of NEWID , create the PK as non-clustered, and get a surrogate key that can fulfill the requirements of a good clustering key.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck15') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck15

SELECT 'Check 15 - Clustered Indexes with GUIDs in key' AS [Info],
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
        a.[Key_has_GUID],
        CASE
            WHEN [is_unique] = 0 AND Index_ID = 1 THEN
                '[WARNING: Clustered index with GUIDs in the key. It is recommended to revise these]'
            ELSE
                'OK'
        END AS [Comment]
   INTO tempdb.dbo.tmpIndexCheck15
   FROM tempdb.dbo.Tab_GetIndexInfo AS a
  WHERE [Key_has_GUID] > 0

SELECT * FROM tempdb.dbo.tmpIndexCheck15
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name