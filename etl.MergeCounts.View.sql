IF OBJECT_ID('[etl].[MergeCounts]', 'V') IS NOT NULL
    DROP VIEW [etl].[MergeCounts];
GO

CREATE VIEW [etl].[MergeCounts]
AS
     WITH rows_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [rows]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'File contains%'
              GROUP BY [procname]),
          inserted_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [inserted]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Inserted new%'
              GROUP BY [procname]),
          updated_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [updated]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Updated changed%'
              GROUP BY [procname]),
          unchanged_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [unchanged]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Unchanged%'
              GROUP BY [procname]),
          primarykey_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [primarykey]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Checked primary key%'
              GROUP BY [procname]),
          nonnull_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [nonnull]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Checked non-nullable%'
              GROUP BY [procname]),
          foreignkey_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [foreignkey]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Checked foreign key%'
              GROUP BY [procname]),
          uniquekey_cte
          AS (SELECT [procname], 
                     SUM([rowcount]) AS [uniquekey]
              FROM [etl].[log]
              WHERE [procname] LIKE 'ap_merge%'
                    AND [message] LIKE 'Checked unique keys%'
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
                 [uniquekey_cte].[uniquekey]
          FROM [rows_cte]
               LEFT OUTER JOIN [inserted_cte] ON [rows_cte].[procname] = [inserted_cte].[procname]
               LEFT OUTER JOIN [updated_cte] ON [rows_cte].[procname] = [updated_cte].[procname]
               LEFT OUTER JOIN [unchanged_cte] ON [rows_cte].[procname] = [unchanged_cte].[procname]
               LEFT OUTER JOIN [primarykey_cte] ON [rows_cte].[procname] = [primarykey_cte].[procname]
               LEFT OUTER JOIN [nonnull_cte] ON [rows_cte].[procname] = [nonnull_cte].[procname]
               LEFT OUTER JOIN [foreignkey_cte] ON [rows_cte].[procname] = [foreignkey_cte].[procname]
               LEFT OUTER JOIN [uniquekey_cte] ON [rows_cte].[procname] = [uniquekey_cte].[procname];
GO