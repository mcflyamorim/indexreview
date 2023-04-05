/* 
Check4 – Index fill-factor

Description:
The fill-factor option is provided for fine-tuning index data storage and performance. When an index is created or rebuilt, the fill-factor value determines the percentage of space on each leaf-level page to be filled with data, reserving the remainder on each page as free space for future growth. For example, specifying a fill-factor value of 80 means that 20 percent of each leaf-level page will be left empty, providing space for index expansion as data is added to the underlying table. The empty space is reserved between the index rows rather than at the end of the index.
A correctly chosen fill-factor value can reduce potential page splits by providing enough space for index expansion as data is added to the underlying table. When a new row is added to a full index page, the Database Engine moves approximately half the rows to a new page to make room for the new row. This reorganization is known as a page split. A page split makes room for new records, but can take time to perform and is a resource intensive operation.
Although a low, nonzero fill-factor value may reduce the requirement to split pages as the index grows, the index will require more storage space and can decrease read performance. Even for an application oriented for many insert and update operations, the number of database reads typically outnumber database writes by a factor of 5 to 10. Therefore, specifying a fill-factor other than the default can decrease database read performance by an amount inversely proportional to the fill-factor setting.

Estimated Benefit:
Very High

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Review index fill-factor and work to increase page density to fit more rows in a page.

Detailed recommendation:
It is particularly important to consider the costs and benefits of setting an index fill-factor other than 100, customers should perform it only when there is a demonstrated/documented need. For instance, setting a fill-factor to 80, will leave 20% of empty space on pages which will make your database 20% larger, table scans take 20% longer, maintenance jobs take 20% longer and your memory 20% smaller (as the empty space on pages are also in memory).
Set fill-factor to 100 to all indexes to avoid internal fragmentation, with exception of cases that there is a demonstrated/documented need.
Make sure that fill-factor of all indexes that first key is monotonically increasing are set to 100. If all the data is added to the end of the table, the empty space in the index pages will not be filled. For example, if the index key column is an IDENTITY column, the key for new rows is always increasing and the index rows are logically added to the end of the index. For those cases, a fill-factor of 100 is the recommended value.
To be able to do a fine tuning (correct balance and tradeoff between decrease read performance vs reduce page-splits and low page density) on the correct fill-factor value for an index, customers can create an session to capture the extended sqlserver.transaction_log event and track mid-page splits in a database. For more details about this, check the following article: https://www.sqlskills.com/blogs/jonathan/tracking-problematic-pages-splits-in-sql-server-2012-extended-events-no-really-this-time/ 

Note: If available memory is enough to keep all the database pages in cache, fragmentation may be less important, but it is still important to use the available resources as best as possible and avoid extra storage space caused by the internal fragmentation.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck4') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck4

SELECT 'Check4 – Index fill-factor' AS [Info],
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
        a.Fill_factor,
        CASE
            WHEN a.Fill_factor BETWEEN 1 AND 79 THEN
                'Index with fill factor lower than 80 percent. Revise the need to maintain such a low value'
            ELSE
                'OK'
        END AS [Comment]
   INTO tempdb.dbo.tmpIndexCheck4
   FROM tempdb.dbo.Tab_GetIndexInfo AS a

SELECT * FROM tempdb.dbo.tmpIndexCheck4
 ORDER BY current_number_of_rows_table DESC, 
          Database_Name,
          Schema_Name,
          Table_Name,
          ReservedSizeInMB DESC,
          Index_Name