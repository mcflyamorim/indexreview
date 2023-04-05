SELECT OBJECT_NAME(id) AS TabeName,
       dpages AS [Pages],
       rowcnt as [Rows],
       CONVERT(BIGINT, ROUND(rowcnt * t7.PercentSampled, 0)) AS [Auto Update/Create Rows Sample],
       CONVERT(BIGINT, t6.SamplePages) AS [Auto Update/Create Pages Sample],
       t7.PercentSampled * 100 AS [Auto Update/Create Percent Sample],
       t1.RowsPerPage
FROM sysindexes
CROSS APPLY (SELECT RowsPerPage = CONVERT(NUMERIC(18,8), rowcnt) / CONVERT(NUMERIC(18,8), dpages)) AS t1
CROSS APPLY (SELECT SampleRows  = CONVERT(NUMERIC(18,8), CEILING(15 * POWER(CONVERT(NUMERIC(18,8), rowcnt), 0.55)))) AS t2
CROSS APPLY (SELECT SampleRate  = CONVERT(NUMERIC(18,8), t2.SampleRows / CONVERT(NUMERIC(18,8), rowcnt))) AS t3
CROSS APPLY (SELECT SamplePages1 = CONVERT(INT, CONVERT(NUMERIC(18,8), dpages) * t3.SampleRate) + 1024) AS t4
CROSS APPLY (SELECT SamplePages2 = CASE WHEN CONVERT(NUMERIC(18,8), dpages) < t4.SamplePages1 THEN CONVERT(NUMERIC(18,8), dpages) ELSE t4.SamplePages1 END) AS t5
CROSS APPLY (SELECT SamplePages = MIN(tab1.Col1) FROM (VALUES(t4.SamplePages1),(t5.SamplePages2)) AS Tab1(Col1)) AS t6
CROSS APPLY (SELECT PercentSampled = t6.SamplePages / dpages) AS t7
WHERE indid <= 1
AND rows > 0
ORDER BY rows DESC
GO
