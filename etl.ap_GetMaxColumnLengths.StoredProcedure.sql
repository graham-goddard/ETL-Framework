IF OBJECT_ID('[etl].[ap_GetMaxColumnLengths]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetMaxColumnLengths];
GO

CREATE PROCEDURE [etl].[ap_GetMaxColumnLengths] 
                 @objectname SYSNAME, 
                 @debug      BIT     = 0
AS
    BEGIN
        SET ANSI_WARNINGS OFF;
        SET ANSI_PADDING OFF;
        DECLARE @schema SYSNAME, @table SYSNAME;
        SELECT @table = PARSENAME(@objectname, 1), 
               @schema = PARSENAME(@objectname, 2);
        DECLARE @cols VARCHAR(MAX), @sql VARCHAR(MAX);
        SELECT @sql = COALESCE(@sql + ',', 'SELECT ''' + @table + ''' AS [TABLE_NAME], ') + 'CAST(MAX(LEN(' + QUOTENAME([COLUMN_NAME]) + ')) AS INT) AS ' + QUOTENAME([COLUMN_NAME]), 
               @cols = COALESCE(@cols + ',', '') + QUOTENAME([COLUMN_NAME])
          FROM [INFORMATION_SCHEMA].[COLUMNS]
         WHERE [TABLE_CATALOG] = DB_NAME()
               AND [TABLE_SCHEMA] = @schema
               AND [TABLE_NAME] = @table
               AND [DATA_TYPE] <> 'xml'
        ORDER BY [ORDINAL_POSITION];
        SELECT @sql = 'SELECT [C].[TABLE_CATALOG],
	[C].[TABLE_SCHEMA],
	[C].[TABLE_NAME],
	[C].[COLUMN_NAME],
	UPPER([C].[DATA_TYPE]) AS [DATA_TYPE],
	[C].[CHARACTER_MAXIMUM_LENGTH] AS [MAX_COLUMN_LENGTH],
    [MAXLEN] AS [MAX_DATA_LENGTH]
	FROM (' + @sql + '
	FROM ' + QUOTENAME(@schema) + '.' + QUOTENAME(@table) + ') P
	UNPIVOT ([MAXLEN] FOR [COLUMN_NAME] IN (' + @cols + ')) AS [U]
	JOIN [INFORMATION_SCHEMA].[COLUMNS] AS [C]
    ON [C].[TABLE_CATALOG] = ''' + DB_NAME() + '''
       AND
       [C].[TABLE_SCHEMA] = ''' + @schema + '''
       AND
       [C].[TABLE_NAME] = ''' + @table + '''
       AND
       [C].[COLUMN_NAME] = [U].[COLUMN_NAME];
;';
        IF @debug = 1
            BEGIN
                PRINT @sql;
        END;
            ELSE
            BEGIN
                EXEC (@sql);
        END;
    END;
GO