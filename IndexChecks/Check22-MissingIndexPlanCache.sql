/* 
Check22 - Missing index plan cache

Description:
Potentially missing indexes were found based on SQL Server query plan cache. It is important to revise them.

Estimated Benefit:
Very High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review recommended missing indexes and if possible, create them.

Detailed recommendation:
Review missing index suggestions to effectively tune indexes and improve query performance. Review the base table structure, carefully combine indexes, consider key column order, and review included column suggestions, examine missing indexes and existing indexes for overlap and avoid creating duplicate indexes.
It's a best practice to review all the missing index requests for a table and the existing indexes on a table before adding an index based on a query execution plan.
Missing index suggestions are best treated as one of several sources of information when performing index analysis, design, tuning, and testing. Missing index suggestions are not prescriptions to create indexes exactly as suggested.
Review the missing index recommendations for a table as a group, along with the definitions of existing indexes on the table. Remember that when defining indexes, generally equality columns should be put before the inequality columns, and together they should form the key of the index. To determine an effective order for the equality columns, order them based on their SELECTivity: list the most SELECTive columns first (leftmost in the column list). Unique columns are most SELECTive, while columns with many repeating values are less SELECTive.
It's important to confirm if your index changes have been successful, “is the query optimizer using your indexes?”. Keep in mind that while indexes can dramatically improve query performance but indexes also have overhead and management costs. 
*/

SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck22') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck22

/* Fabiano Amorim  */
/* http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com */
SET NOCOUNT ON; SET ANSI_WARNINGS ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 50; /*if I get blocked for more than 50ms I'll quit, I don't want to wait or cause other blocks*/

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  'Check22 - Missing index plan cache' AS [Info],
        qp.*,
        index_impact,
        ix_db + '.' + ix_schema + '.' + ix_table AS table_name,
        key_cols,
        include_cols,
        t2.create_index_command
INTO tempdb.dbo.tmpIndexCheck22
FROM tempdb.dbo.tmpIndexCheckCachePlanData qp
OUTER APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
CROSS APPLY
  --Find the Missing Indexes Group Nodes in the Plan
  x.nodes('.//p:MissingIndexGroup') F_GrpNodes(GrpNode)
CROSS APPLY 
  --Pull out the Impact Figure
  (SELECT index_impact = GrpNode.value('(./@Impact)','float')) F_Impact
CROSS APPLY 
  --Get the Missing Index Nodes from the Group
  GrpNode.nodes('(./p:MissingIndex)') F_IxNodes(IxNode)
CROSS APPLY 
  --Pull out the Database,Schema,Table of the Missing Index
  (SELECT ix_db=IxNode.value('(./@Database)','sysname')
         ,ix_schema=IxNode.value('(./@Schema)','sysname')
         ,ix_table=IxNode.value('(./@Table)','sysname')
  ) F_IxInfo
CROSS APPLY 
  --Pull out the Key Columns and the Include Columns from the various Column Groups
  (SELECT eq_cols=MAX(CASE WHEN Usage='EQUALITY' THEN ColList END)
         ,ineq_cols=MAX(CASE WHEN Usage='INEQUALITY' THEN ColList END)
         ,include_cols=MAX(CASE WHEN Usage='INCLUDE' THEN ColList END)
   FROM IxNode.nodes('(./p:ColumnGroup)') F_ColGrp(ColGrpNode)
   CROSS APPLY 
     --Pull out the Usage of the Group? (EQUALITY of INEQUALITY or INCLUDE)
     (SELECT Usage=ColGrpNode.value('(./@Usage)','varchar(20)')) F_Usage
   CROSS APPLY 
     --Get a comma-delimited list of the Column Names in the Group
     (SELECT ColList=stuff((SELECT ','+ColNode.value('(./@Name)','sysname')
                            FROM ColGrpNode.nodes('(./p:Column)') F_ColNodes(ColNode)
                            FOR XML PATH(''))
                          ,1,1,'')
     ) F_ColList
  ) F_ColGrps
CROSS APPLY
  --Put together the Equality and InEquality Columns
  (SELECT key_cols=isnull(eq_cols,'')
                 +case 
                    when eq_cols is not null and ineq_cols is not null 
                    then ',' 
                    else '' 
                  end
                 +isnull(ineq_cols,'')
  ) F_KeyCols
CROSS APPLY 
  --Construct a CREATE INDEX command
  (SELECT create_index_command='USE ' + ix_db + ';' + NCHAR(13) + NCHAR(10) + 'GO' + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10)
                               + 'CREATE INDEX [<Name of Missing Index>] ON ' + ix_db + '.' + ix_schema + '.' + ix_table + ' (' + key_cols +')' 
                               + NCHAR(13) + NCHAR(10)
                               + ISNULL('INCLUDE (' + include_cols + ')' 
                               + NCHAR(13) + NCHAR(10),'')
                               + 'WITH(ONLINE = ON)' + NCHAR(13) + NCHAR(10) 
                               + 'GO' + NCHAR(13) + NCHAR(10)
                               ) F_Cmd 
OUTER APPLY (SELECT CONVERT(XML, ISNULL(CONVERT(XML, '<?index --' +
                                                        REPLACE
					                                                   (
						                                                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                   CONVERT
							                                                   (
								                                                   VARCHAR(MAX),
								                                                   N'--' + NCHAR(13) + NCHAR(10) + F_Cmd.create_index_command + N'--' COLLATE Latin1_General_Bin2
							                                                   ),
							                                                   NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                   NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                   NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                   NCHAR(0),
						                                                   N'')
                                                         + '--?>'),
                                              '<?index --' + NCHAR(13) + NCHAR(10) +
                                              'Statement not found.' + NCHAR(13) + NCHAR(10) +
                                              '--?>'))) AS t2 (create_index_command)
OPTION (RECOMPILE, MAXDOP 4);

SELECT * FROM tempdb.dbo.tmpIndexCheck22
ORDER BY index_impact DESC