/* 
Check14 – Non-unique clustered index

Description:
Index uniqueness is highly desirable attribute of a clustering key, and goes hand-in-hand with index narrowness. SQL Server does not require a clustered index to be unique, but yet it must have some means of uniquely identifying every row. For non-unique clustered indexes, SQL Server adds to every duplicate instance of a clustering key value a 4-byte integer value called a uniqueifier. This uniqueifier is added everywhere the clustering key is stored. That means the uniqueifier is stored in both clustered and non-clustered indexes. 
Non-unique clustered indexes will have an additional overhead at index creation as there's extra disk space and additional costs on INSERTs and UPDATEs.

Estimated Benefit:
Low

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review indexes cluster key and if possible, use a unique key.

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
•	Use JOIN clauses; typically these are foreign key columns.
•	Use ORDER BY or GROUP BY clauses.
•	An index on the columns specified in the ORDER BY or GROUP BY clause may remove the need for the Database Engine to sort the data, because the rows are already sorted. This improves query performance.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck14') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck14

SELECT 'Check 14 - Non-unique clustered indexes' AS [Info],
        a.Database_Name,
        a.Schema_Name,
        a.Table_Name,
        a.Index_Name,
        a.Index_Type,
        a.Indexed_Columns,
        a.Number_Rows AS current_number_of_rows_table,
        a.ReservedSizeInMB,
        user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
        a.last_datetime_obj_was_used,
        a.[is_unique],
        CASE
            WHEN [is_unique] = 0 AND index_ID = 1 THEN
                '[WARNING: Clustered index is non-unique. Revise the need to have non-unique clustering keys to which a uniquefier is added]'
            ELSE
                'OK'
        END AS [Comment]
   INTO tempdb.dbo.tmpIndexCheck14
   FROM tempdb.dbo.Tab_GetIndexInfo AS a
  WHERE [is_unique] = 0 AND index_ID = 1

SELECT * FROM tempdb.dbo.tmpIndexCheck14
 ORDER BY current_number_of_rows_table DESC, 
          Database_Name,
          Schema_Name,
          Table_Name,
          ReservedSizeInMB DESC,
          Index_Name