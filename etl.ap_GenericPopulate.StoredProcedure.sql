IF OBJECT_ID('[etl].[ap_GenericPopulate]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GenericPopulate];
GO

CREATE PROCEDURE [etl].[ap_GenericPopulate] 
                 @source_table  SYSNAME, 
                 @dest_table    SYSNAME     = NULL, 
                 @source_schema SYSNAME     = 'EXTRACTS', 
                 @dest_schema   SYSNAME     = 'TRANSFORMS', 
                 @truncate      BIT         = 0, 
                 @matchon       VARCHAR(10) = 'NAME', -- POSITION or NAME
                 @convertnull   BIT         = 0, 
                 @rtrim         BIT         = 0, 
                 @debug         BIT         = 0
AS
    BEGIN
        DECLARE @source_cols VARCHAR(MAX);
        DECLARE @dest_cols VARCHAR(MAX);
        DECLARE @execsql NVARCHAR(MAX);
        DECLARE @sql VARCHAR(MAX);
        DECLARE @sql1 VARCHAR(4000);
        DECLARE @sql2 VARCHAR(4000);
        DECLARE @sql3 VARCHAR(4000);
        DECLARE @sql4 VARCHAR(4000);
        DECLARE @truncate_sql VARCHAR(4000);
        IF @dest_table IS NULL
            BEGIN
                SET @dest_table = @source_table;
        END;
        DECLARE @rc INT, @type TINYINT, @logtime DATETIME, @rowcount INT, @procname VARCHAR(128), @source VARCHAR(128), @destination VARCHAR(128), @message VARCHAR(2048), @logxml XML;
        SELECT @type = 0, 
               @logtime = GETDATE(), 
               @procname = OBJECT_NAME(@@procid), 
               @source = QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table), 
               @destination = QUOTENAME(@dest_schema) + '.' + QUOTENAME(@dest_table), 
               @message = 'Started task';
        IF @debug = 0
            BEGIN
                EXECUTE @rc = [etl].[ap_InsertETLlog] 
                        @type, 
                        @logtime OUTPUT, 
                        @rowcount, 
                        @procname, 
                        @source, 
                        @destination, 
                        @message;
        END;
        --
        -- Check for string truncation
        --
        DECLARE @dest_column_name SYSNAME, @source_column_name SYSNAME, @character_maximum_length INT, @dest_ordinal_position INT;
        DECLARE csrStringTruncate CURSOR
        FOR SELECT [COLUMN_NAME], 
                   [CHARACTER_MAXIMUM_LENGTH], 
                   [ORDINAL_POSITION]
              FROM [INFORMATION_SCHEMA].[COLUMNS]
             WHERE [TABLE_CATALOG] = DB_NAME()
                   AND [TABLE_SCHEMA] = @dest_schema
                   AND [TABLE_NAME] = @dest_table
                   AND COLUMNPROPERTY(OBJECT_ID([TABLE_SCHEMA] + '.' + [TABLE_NAME]), [COLUMN_NAME], 'IsComputed') = 0
                   AND [CHARACTER_MAXIMUM_LENGTH] <> -1
                   AND [DATA_TYPE] IN('varchar', 'nvarchar', 'char', 'nchar')
            AND [COLUMN_NAME] NOT IN('FILENAME', 'ROWNUMBER')
            ORDER BY [ORDINAL_POSITION];
        OPEN csrStringTruncate;
        FETCH NEXT FROM csrStringTruncate INTO @dest_column_name, @character_maximum_length, @dest_ordinal_position;
        WHILE @@fetch_status = 0
            BEGIN
                SELECT @source_column_name = [COLUMN_NAME]
                  FROM [INFORMATION_SCHEMA].[COLUMNS]
                 WHERE [TABLE_CATALOG] = DB_NAME()
                       AND [TABLE_SCHEMA] = @source_schema
                       AND [TABLE_NAME] = @source_table
                       AND ((@matchon = 'NAME'
                             AND [COLUMN_NAME] = @dest_column_name)
                            OR (@matchon = 'POSITION'
                                AND [ORDINAL_POSITION] = @dest_ordinal_position));
                SELECT @execsql = 'SELECT @logXML = (SELECT [FILENAME], [ROWNUMBER], ' + QUOTENAME(@source_column_name) + ' FROM ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + ' WHERE LEN(' + QUOTENAME(@source_column_name) + ') > ' + LTRIM(STR(@character_maximum_length)) + ' FOR XML RAW(''TRUNCATED'') , ROOT); SELECT @rowcount = @logXML.value(''count(/root/TRUNCATED)'', ''int'')';
                EXEC [sp_executesql] 
                     @execsql, 
                     N'@logXML XML OUTPUT, @rowcount INT OUTPUT', 
                     @logXML OUTPUT, 
                     @rowcount OUTPUT;
                IF DATALENGTH(@logXML) IS NOT NULL
                    BEGIN
                        SELECT @type = 1, 
                               @message = FORMATMESSAGE('String data truncated on %s records in %s.%s.%s', FORMAT(@rowcount, 'N0'), QUOTENAME(@dest_schema), QUOTENAME(@dest_table), QUOTENAME(@dest_column_name));
                        IF DATALENGTH(@logXML) > 1048576 -- 1MB
                            BEGIN
                                SELECT @message = FORMATMESSAGE(@message + ' (LogXML too large to write to log %s bytes )', FORMAT(DATALENGTH(@logXML), 'N0'));
                                SET @logXML = NULL;
                        END;
                        EXECUTE @rc = [ETL].[ap_InsertETLlog] 
                                @type, 
                                @logtime OUTPUT, 
                                @rowcount, 
                                @procname, 
                                @source, 
                                @destination, 
                                @message, 
                                @logXML;
                        SET @type = 0;
                END;
                FETCH NEXT FROM csrStringTruncate INTO @dest_column_name, @character_maximum_length, @dest_ordinal_position;
            END;
        CLOSE csrStringTruncate;
        DEALLOCATE csrStringTruncate;
        --
        --
        --
        SELECT @dest_cols = COALESCE(@dest_cols + ',', '') + '[' + [COLUMN_NAME] + ']'
          FROM [INFORMATION_SCHEMA].[COLUMNS]
         WHERE [TABLE_CATALOG] = DB_NAME()
               AND [TABLE_SCHEMA] = @dest_schema
               AND [TABLE_NAME] = @dest_table
               AND COLUMNPROPERTY(OBJECT_ID([TABLE_SCHEMA] + '.' + [TABLE_NAME]), [COLUMN_NAME], 'IsComputed') = 0
        ORDER BY [ORDINAL_POSITION];
        SELECT @source_cols = COALESCE(@source_cols + ',', '') + [etl].[fn_GetCastString]
               ([S].[DATA_TYPE], [D].[DATA_TYPE], [S].[COLUMN_NAME], [D].[COLUMN_NAME], [D].[NUMERIC_PRECISION], [D].[NUMERIC_SCALE], [D].[CHARACTER_MAXIMUM_LENGTH], @convertnull, @rtrim)
          FROM [INFORMATION_SCHEMA].[COLUMNS] AS [D]
               LEFT OUTER JOIN [INFORMATION_SCHEMA].[COLUMNS] AS [S] ON [S].[TABLE_CATALOG] = DB_NAME()
                                                                        AND [S].[TABLE_SCHEMA] = @source_schema
                                                                        AND [S].[TABLE_NAME] = @source_table
                                                                        AND ((@matchon = 'POSITION'
                                                                              AND [S].[ORDINAL_POSITION] = [D].[ORDINAL_POSITION])
                                                                             OR (@matchon = 'NAME'
                                                                                 AND [S].[COLUMN_NAME] = [D].[COLUMN_NAME]))
         WHERE [D].[TABLE_CATALOG] = DB_NAME()
               AND [D].[TABLE_SCHEMA] = @dest_schema
               AND [D].[TABLE_NAME] = @dest_table
               AND COLUMNPROPERTY(OBJECT_ID([D].[TABLE_SCHEMA] + '.' + [D].[TABLE_NAME]), [D].[COLUMN_NAME], 'IsComputed') = 0
        ORDER BY [D].[ORDINAL_POSITION];
        IF @@rowcount = 0
            BEGIN
                RAISERROR(N'No data copied. Check parameter values', 16, 1);
        END;
        SELECT @sql = 'INSERT ' + QUOTENAME(@dest_schema) + '.' + QUOTENAME(@dest_table) + ' ' + '(' + @dest_cols + ') SELECT ' + @source_cols + ' FROM ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table);
        SELECT @sql1 = COALESCE(SUBSTRING(@sql, 1, 4000), '');
        SELECT @sql2 = COALESCE(SUBSTRING(@sql, 4001, 8000), '');
        SELECT @sql3 = COALESCE(SUBSTRING(@sql, 8001, 12000), '');
        SELECT @sql4 = COALESCE(SUBSTRING(@sql, 12001, 16000), '');
        IF @debug = 0
            BEGIN
                IF @truncate = 1
                    BEGIN
                        SET @truncate_sql = 'TRUNCATE TABLE ' + QUOTENAME(@dest_schema) + '.' + QUOTENAME(@dest_table);
                        EXEC (@truncate_sql);
                END;
                EXEC (@sql1+@sql2+@sql3+@sql4);
                SELECT @rowcount = @@rowcount, 
                       @message = 'Inserted ' + LTRIM(STR(@rowcount)) + ' records into ' + QUOTENAME(@dest_schema) + '.' + QUOTENAME(@dest_table);
                IF @debug = 0
                    BEGIN
                        EXECUTE @rc = [etl].[ap_InsertETLlog] 
                                @type, 
                                @logtime OUTPUT, 
                                @rowcount, 
                                @procname, 
                                @source, 
                                @destination, 
                                @message;
                END;
        END;
            ELSE
            BEGIN
                PRINT '-- Running in debug mode - no data changed';
                PRINT @sql1;
                PRINT @sql2;
                PRINT @sql3;
                PRINT @sql4;
        END;
        IF @source_cols LIKE '%/*UNMAPPED%'
            BEGIN
                SELECT @type = 1, 
                       @logtime = GETDATE(), 
                       @procname = OBJECT_NAME(@@procid), 
                       @source = @source_schema + '.' + @source_table, 
                       @destination = @dest_schema + '.' + @dest_table, 
                       @message = 'WARNING! Unmapped columns. Refer to XML output for details.';
                IF @debug = 0
                    BEGIN
                        SET @logxml = (
                            SELECT @sql AS [SQL] FOR XML RAW('SQLCMD'), ROOT
                            );
                        EXECUTE @rc = [etl].[ap_InsertETLlog] 
                                @type, 
                                @logtime OUTPUT, 
                                @rowcount, 
                                @procname, 
                                @source, 
                                @destination, 
                                @message, 
                                @logxml;
                END;
        END;
        SELECT @rowcount = NULL, 
               @type = 0, 
               @message = 'Finished task';
        IF @debug = 0
            BEGIN
                EXECUTE @rc = [etl].[ap_InsertETLlog] 
                        @type, 
                        @logtime OUTPUT, 
                        @rowcount, 
                        @procname, 
                        @source, 
                        @destination, 
                        @message;
        END;
        RETURN;
    END;
GO