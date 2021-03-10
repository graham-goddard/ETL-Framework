
IF OBJECT_ID('[etl].[fn_MergeCounts]', 'IF') IS NOT NULL
    DROP FUNCTION [etl].[fn_MergeCounts];
GO
CREATE FUNCTION [etl].[fn_MergeCounts](@id INT
)
RETURNS TABLE
AS
     RETURN(
     WITH rows_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [rows]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'File contains%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          inserted_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [inserted]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Inserted new%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          updated_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [updated]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Updated changed%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          unchanged_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [unchanged]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Unchanged%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          primarykey_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [primarykey]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Checked primary key%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          nonnull_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [nonnull]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Checked non-nullable%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          overlap_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [overlap]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Checked overlapping%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          foreignkey_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [foreignkey]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Checked foreign key%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname]),
          uniquekey_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [uniquekey]
                FROM [etl].[log]
               WHERE [procname] LIKE 'ap_merge%'
                     AND [message] LIKE 'Checked unique keys%'
                     AND [id] >= COALESCE(@id, 0)
               GROUP BY [procname])
          SELECT [rows_cte].[procname], 
                 [rows_cte].[rows], 
                 [inserted_cte].[inserted], 
                 [updated_cte].[updated], 
                 [unchanged_cte].[unchanged], 
                 [rows_cte].[rows] - [inserted_cte].[inserted] - [updated_cte].[updated] - [unchanged_cte].[unchanged] AS [exceptions], 
                 [primarykey_cte].[primarykey], 
                 [nonnull_cte].[nonnull], 
                 [foreignkey_cte].[foreignkey], 
                 [uniquekey_cte].[uniquekey], 
                 [overlap_cte].[overlap]
            FROM [rows_cte]
                 LEFT OUTER JOIN [inserted_cte] ON [rows_cte].[procname] = [inserted_cte].[procname]
                 LEFT OUTER JOIN [updated_cte] ON [rows_cte].[procname] = [updated_cte].[procname]
                 LEFT OUTER JOIN [unchanged_cte] ON [rows_cte].[procname] = [unchanged_cte].[procname]
                 LEFT OUTER JOIN [primarykey_cte] ON [rows_cte].[procname] = [primarykey_cte].[procname]
                 LEFT OUTER JOIN [nonnull_cte] ON [rows_cte].[procname] = [nonnull_cte].[procname]
                 LEFT OUTER JOIN [foreignkey_cte] ON [rows_cte].[procname] = [foreignkey_cte].[procname]
                 LEFT OUTER JOIN [uniquekey_cte] ON [rows_cte].[procname] = [uniquekey_cte].[procname]
                 LEFT OUTER JOIN [overlap_cte] ON [rows_cte].[procname] = [overlap_cte].[procname]);
GO