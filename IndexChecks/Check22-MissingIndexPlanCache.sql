/* 
Check22 – Missing index plan cache

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
Review the missing index recommendations for a table as a group, along with the definitions of existing indexes on the table. Remember that when defining indexes, generally equality columns should be put before the inequality columns, and together they should form the key of the index. To determine an effective order for the equality columns, order them based on their selectivity: list the most selective columns first (leftmost in the column list). Unique columns are most selective, while columns with many repeating values are less selective.
It's important to confirm if your index changes have been successful, “is the query optimizer using your indexes?”. Keep in mind that while indexes can dramatically improve query performance but indexes also have overhead and management costs. 
*/



SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck22') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck22

;with xmlnamespaces 
(
  default 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'
)
select TOP 1000
       'Check22 – Missing index plan cache' AS [Info],
       Impact
      ,TableName=IxDB+'.'+IxSchema+'.'+IxTable
      ,KeyCols
      ,IncludeCols
      ,IndexCommand
      ,usecounts
      ,NumKeys
      ,NumIncludes
      ,size_in_bytes
      ,objtype
      ,BatchCode
      --,QueryPlan=qp.query_plan 
INTO tempdb.dbo.tmpIndexCheck22
FROM sys.dm_exec_cached_plans qs 
CROSS APPLY 
  --Get the Query Text
  sys.dm_exec_sql_text(qs.plan_handle) qt             
CROSS APPLY 
  --Get the Query Plan
  sys.dm_exec_query_plan(qs.plan_handle) qp
CROSS APPLY
  --Get the Code for the Batch in Hyperlink Form
  (SELECT BatchCode
            =(SELECT [processing-instruction(q)]=':'+NCHAR(13)+qt.text+NCHAR(13)
              FOR XML PATH(''),TYPE)
  ) F_Code
CROSS APPLY
  --Find the Missing Indexes Group Nodes in the Plan
  qp.query_plan.nodes('//MissingIndexes/MissingIndexGroup') F_GrpNodes(GrpNode)
CROSS APPLY 
  --Pull out the Impact Figure
  (SELECT Impact=GrpNode.value('(./@Impact)','float')) F_Impact
CROSS APPLY 
  --Get the Missing Index Nodes from the Group
  GrpNode.nodes('(./MissingIndex)') F_IxNodes(IxNode)
CROSS APPLY 
  --Pull out the Database,Schema,Table of the Missing Index
  (SELECT IxDB=IxNode.value('(./@Database)','sysname')
         ,IxSchema=IxNode.value('(./@Schema)','sysname')
         ,IxTable=IxNode.value('(./@Table)','sysname')
  ) F_IxInfo
CROSS APPLY 
  --How many INCLUDE columns are there;
  --And how many EQUALITY/INEQUALITY columns are there?
  (SELECT NumIncludes
            =IxNode.value('count(./ColumnGroup[@Usage="INCLUDE"]/Column)','int')
         ,NumKeys
            =IxNode.value('count(./ColumnGroup[@Usage!="INCLUDE"]/Column)','int')
  ) F_NumIncl
CROSS APPLY 
  --Pull out the Key Columns and the Include Columns from the various Column Groups
  (SELECT EqCols=MAX(CASE WHEN Usage='EQUALITY' THEN ColList END)
         ,InEqCols=MAX(CASE WHEN Usage='INEQUALITY' THEN ColList END)
         ,IncludeCols=MAX(CASE WHEN Usage='INCLUDE' THEN ColList END)
   FROM IxNode.nodes('(./ColumnGroup)') F_ColGrp(ColGrpNode)
   CROSS APPLY 
     --Pull out the Usage of the Group? (EQUALITY of INEQUALITY or INCLUDE)
     (SELECT Usage=ColGrpNode.value('(./@Usage)','varchar(20)')) F_Usage
   CROSS APPLY 
     --Get a comma-delimited list of the Column Names in the Group
     (select ColList=stuff((select ','+ColNode.value('(./@Name)','sysname')
                            from ColGrpNode.nodes('(./Column)') F_ColNodes(ColNode)
                            for xml path(''))
                          ,1,1,'')
     ) F_ColList
  ) F_ColGrps
cross apply
  --Put together the Equality and InEquality Columns
  (select KeyCols=isnull(EqCols,'')
                 +case 
                    when EqCols is not null and InEqCols is not null 
                    then ',' 
                    else '' 
                  end
                 +isnull(InEqCols,'')
  ) F_KeyCols
cross apply 
  --Construct a CREATE INDEX command
  (select IndexCommand='create index <InsertNameHere> on '
                      +IxDB+'.'+IxSchema+'.'+IxTable+' ('
                      +KeyCols+')'
                      +isnull(' include ('+IncludeCols+')','')) F_Cmd
where qs.cacheobjtype='Compiled Plan'
  --and usecounts>=5    --Only interested in those plans used at least 5 times
  --and NumKeys<=5      --Limit to the #columns we're willing to have in the index
  --and NumIncludes<=5  --Limit to the #columns we're willing to have in the INCLUDE list
  --and Impact>=50      --Only indexes that will have a 50% impact
order by Impact DESC

SELECT * FROM tempdb.dbo.tmpIndexCheck22
order by Impact DESC
