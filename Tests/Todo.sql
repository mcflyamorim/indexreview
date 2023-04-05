Add new check:
Find Best Clustered Index - https://sqlenlight.com/support/help/ex0015/
The rule uses the DMV-s to identify the non-clustered indexes that are candidates to become clustered indexes.
The non-clustered indexes are considered better compared to the existing clustered indexes if the number of user seeks on those indexes 
is greater than the number of lookups on the related to the table clustered index.


SELECT * FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim


-- Generate excel file with spreadsheet for each check

-- Check if there are FKs with multi columns
-- if so, they probably should have an index or at least an multi column
-- stats to help query optimizer to create query plans optimized accurately.


-- Check if tables are too big (greater than 10mi) and recommend 
-- partitioning + increment + update stats with ON PARTITIONS option
-- The idea is to have table partitioned by date and update only the latest
-- partitions...
-- Check if table also has an ascending key to be used as a partition key... Maybe a date or a identity.


-- Check if tables are getting close to 8MB, as once they do, SQL will stop to do
-- a fullscan on auto update stats and start to use a sample rate.
-- I still need to confirm this is the way it works... so, I'll have to check this... 

-- Add a check to validade IF stat(index_stat) has A anti-matter column as this may cause issues with update stats

-- Create proc to deal with async execution - Create check to see how many simultaneously could be running  

-- Based on top 20 plan cache querias... grab stats and check Last update vs last executed and number of rows when it as updated vs current number of rows... 
-- the idea is to find outdated stats that are being used...

-- Recommend extented events to collect more information about updatestats... Provide script to create xevent, https://sqlperformance.com/2014/04/extended-events/tracking-auto-stats
