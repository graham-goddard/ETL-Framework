-- Package: etl  
-- Version: 01.14.0000 
 
PRINT '\etl.drop.Script.sql' 
GO 
 
IF OBJECT_ID('[etl].[MergeCounts]', 'V') IS NOT NULL
    DROP VIEW [etl].[MergeCounts];
GO

IF OBJECT_ID('[etl].[logXML]', 'U') IS NOT NULL
    DROP TABLE [etl].[logXML];
GO

IF OBJECT_ID('[etl].[log]', 'U') IS NOT NULL
    DROP TABLE [etl].[log];
GO

IF OBJECT_ID('[etl].[ap_WriteStringToFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_WriteStringToFile];
GO

IF OBJECT_ID('[etl].[ap_InsertETLlog]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_InsertETLlog];
GO

IF OBJECT_ID('[etl].[ap_GenericPopulate]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GenericPopulate];
GO

IF OBJECT_ID('[etl].[ap_GetFirstRowFromFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetFirstRowFromFile];
GO

IF OBJECT_ID('[etl].[ap_ImportFiles]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_ImportFiles];
GO

IF OBJECT_ID('[etl].[ap_ImportExcel]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_ImportExcel];
GO

IF OBJECT_ID('[etl].[ap_GetMaxColumnLengths]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetMaxColumnLengths];
GO

IF OBJECT_ID('[etl].[FileCopy]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileCopy];
GO

IF OBJECT_ID('[etl].[FileDelete]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileDelete];
GO

IF OBJECT_ID('[etl].[FileDeleteMatch]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileDeleteMatch];
GO

IF OBJECT_ID('[etl].[FileMove]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileMove];
GO

IF OBJECT_ID('[etl].[FileReplace]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileReplace];
GO

IF OBJECT_ID('[etl].[fn_GetCastString]', 'FN') IS NOT NULL
    DROP FUNCTION [etl].[fn_GetCastString];
GO
 
PRINT '\etl.createschema.sql' 
GO 
 
IF NOT EXISTS
(
    SELECT *
    FROM [sys].[schemas]
    WHERE [name] = 'etl'
)
	EXECUTE('CREATE SCHEMA [etl]');
GO 
PRINT '\etl.log.Table.sql' 
GO 
 
IF OBJECT_ID('[etl].[logXML]', 'U') IS NOT NULL
    DROP TABLE [etl].[logXML];
GO

IF OBJECT_ID('[etl].[log]', 'U') IS NOT NULL
    DROP TABLE [etl].[log];
GO

CREATE TABLE [etl].[log](
             [id]          [INT] IDENTITY(1, 1) NOT NULL, 
             [type]        [TINYINT] NOT NULL, 
             [starttime]   [DATETIME] NOT NULL, 
             [endtime]     [DATETIME] NOT NULL, 
             [duration] AS (DATEDIFF([millisecond], [starttime], [endtime])), 
             [rowcount]    [INT] NULL, 
             [spid]        [INT] NOT NULL, 
             [username]    [SYSNAME] NOT NULL, 
             [nestlevel]   [INT] NOT NULL, 
             [procname]    [VARCHAR](128) NULL, 
             [source]      [VARCHAR](128) NULL, 
             [destination] [VARCHAR](128) NULL, 
             [message]     [VARCHAR](2048) NOT NULL, 
             CONSTRAINT [PK_ETL_log] PRIMARY KEY CLUSTERED([id] ASC)
             WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
)
ON [PRIMARY];
GO

ALTER TABLE [etl].[log]
WITH CHECK
ADD CONSTRAINT [CK_ETL_log] CHECK(([type] = (2)
                                   OR [type] = (1)
                                   OR [type] = (0)));
GO

ALTER TABLE [etl].[log] CHECK CONSTRAINT [CK_ETL_log];
GO

CREATE TABLE [etl].[logXML]
             (
             [id]      [INT] NOT NULL,
             [logXML]  [XML] NOT NULL,
             CONSTRAINT [PK_ETL_logXML] PRIMARY KEY CLUSTERED([id] ASC)
             WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
             )
ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

ALTER TABLE [etl].[logXML]
WITH CHECK
ADD CONSTRAINT [FK_log_logXML] FOREIGN KEY([id]) REFERENCES [etl].[log]([id]);
GO

ALTER TABLE [etl].[logXML] CHECK CONSTRAINT [FK_log_logXML];
GO
 
PRINT '\etl.fn_GetCastString.Function.sql' 
GO 
 
IF OBJECT_ID('[etl].[fn_GetCastString]', 'FN') IS NOT NULL
    DROP FUNCTION [etl].[fn_GetCastString];
GO

CREATE FUNCTION [etl].[fn_GetCastString](
                @sourcedatatype    SYSNAME, 
                @destdatatype      SYSNAME, 
                @sourcecolname     SYSNAME, 
                @destcolname       SYSNAME, 
                @destprecision     INT, 
                @destscale         INT, 
                @destcharmaxlength INT, 
                @convertnull       BIT, 
                @rtrim             BIT
)
RETURNS VARCHAR(MAX)
AS
     BEGIN
         DECLARE @output VARCHAR(8000);
         DECLARE @sourcecolname2 VARCHAR(8000);
         SELECT @sourcecolname2 = '[' + @sourcecolname + ']';
         IF @convertnull = 1
            AND @sourcedatatype = 'nvarchar'
             BEGIN
                 SELECT @sourcecolname2 = 'NULLIF([' + @sourcecolname + '],''NULL'')';
         END;
         SELECT @sourcecolname = @sourcecolname2;
         SELECT @output = CASE
                              WHEN @sourcedatatype IS NULL THEN 'NULL /*UNMAPPED COLUMN ' + QUOTENAME(@destcolname) + '*/ '
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype IN('nvarchar', 'nchar', 'varchar', 'char') THEN CASE
                                                                                                         WHEN @rtrim = 1 THEN 'RTRIM('
                                                                                                         ELSE ''
                                                                                                     END + 'TRY_CAST (' + @sourcecolname + ' AS ' + UPPER(@destdatatype) + '(' + COALESCE(LTRIM(STR(NULLIF(@destcharmaxlength, -1))), 'max') + '))' + CASE
                                                                                                                                                                                                                                                          WHEN @rtrim = 1 THEN ')'
                                                                                                                                                                                                                                                          ELSE ''
                                                                                                                                                                                                                                                      END
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'int' THEN 'TRY_CAST (' + @sourcecolname + ' AS int)'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'money' THEN 'TRY_CAST (' + @sourcecolname + ' AS money)'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'numeric' THEN 'TRY_CAST (' + @sourcecolname + ' AS numeric(' + LTRIM(STR(@destprecision)) + ',' + LTRIM(STR(@destscale)) + '))'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'datetime' THEN 'TRY_PARSE (' + @sourcecolname + ' AS datetime USING ''EN-GB'')'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'date' THEN 'TRY_PARSE (' + @sourcecolname + ' AS date USING ''EN-GB'')'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'time' THEN 'TRY_PARSE (' + @sourcecolname + ' AS time USING ''EN-GB'')'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'bit' THEN 'TRY_CAST (CASE WHEN ' + @sourcecolname + ' IN (''Y'',''Yes'',''True'',''T'',''1'') THEN 1 WHEN ' + @sourcecolname + ' IN (''N'',''No'',''False'',''F'',''0'') THEN 0 ELSE NULL END AS bit)'
                              ELSE '' + @sourcecolname + ''
                          END;
         RETURN @output;
     END;
GO 
PRINT '\etl.FileCopy.Function.sql' 
GO 
 
IF OBJECT_ID('[etl].[FileCopy]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileCopy];
GO

CREATE FUNCTION [etl].[FileCopy](
                @SourceFileName [NVARCHAR](4000), 
                @DestFileName   [NVARCHAR](4000), 
                @Overwrite      [BIT]
)
RETURNS [NVARCHAR](4000)
WITH EXECUTE AS CALLER
AS
     EXTERNAL NAME
[FileIO].[FileIO].[FileCopy];
GO 
PRINT '\etl.FileDelete.Function.sql' 
GO 
 
IF OBJECT_ID('[etl].[FileDelete]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileDelete];
GO

CREATE FUNCTION [etl].[FileDelete](
                @Path [NVARCHAR](4000)
)
RETURNS [NVARCHAR](4000)
WITH EXECUTE AS CALLER
AS
     EXTERNAL NAME
[FileIO].[FileIO].[FileDelete];
GO 
PRINT '\etl.FileDeleteMatch.Function.sql' 
GO 
 
IF OBJECT_ID('[etl].[FileDeleteMatch]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileDeleteMatch];
GO

CREATE FUNCTION [etl].[FileDeleteMatch](
                @DirectoryPath  [NVARCHAR](4000), 
                @SearchPattern  [NVARCHAR](4000), 
                @Subdirectories [BIT], 
                @Match          [BIT]
)
RETURNS [NVARCHAR](4000)
WITH EXECUTE AS CALLER
AS
     EXTERNAL NAME
[FileIO].[FileIO].[FileDeleteMatch];
GO 
PRINT '\etl.FileMove.Function.sql' 
GO 
 
IF OBJECT_ID('[etl].[FileMove]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileMove];
GO

CREATE FUNCTION [etl].[FileMove](
                @SourceFileName [NVARCHAR](4000), 
                @DestFileName   [NVARCHAR](4000)
)
RETURNS [NVARCHAR](4000)
WITH EXECUTE AS CALLER
AS
     EXTERNAL NAME
[FileIO].[FileIO].[FileMove];
GO 
PRINT '\etl.FileReplace.Function.sql' 
GO 
 
IF OBJECT_ID('[etl].[FileReplace]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileReplace];
GO

CREATE FUNCTION [etl].[FileReplace](
                @SourceFileName       [NVARCHAR](4000), 
                @DestFileName         [NVARCHAR](4000), 
                @BackupFileName       [NVARCHAR](4000), 
                @IgnoreMetadataErrors [BIT]
)
RETURNS [NVARCHAR](4000)
WITH EXECUTE AS CALLER
AS
     EXTERNAL NAME
[FileIO].[FileIO].[FileReplace];
GO 
PRINT '\etl.fn_MergeCounts.Function.sql' 
GO 
 

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
PRINT '\etl.ap_WriteStringToFile.StoredProcedure.sql' 
GO 
 
IF OBJECT_ID('[etl].[ap_WriteStringToFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_WriteStringToFile];
GO

CREATE PROCEDURE [etl].[ap_WriteStringToFile](
                 @string    VARCHAR(MAX), --8000 in SQL Server 2000
                 @path      VARCHAR(260), 
                 @filename  VARCHAR(260), 
                 @overwrite BIT          = 1
)
AS
    BEGIN
        DECLARE @objFileSystem INT, @objTextStream INT, @objErrorObject INT, @strErrorMessage VARCHAR(1000), @command VARCHAR(1000), @hr INT, @fileAndPath VARCHAR(520), @isExists INT;
        SET NOCOUNT ON;
        SELECT @fileAndPath = @path + '\' + @filename;
        EXEC [master].[dbo].[xp_fileexist] 
             @fileAndPath, 
             @isExists OUTPUT;
        IF @isExists = 1
           AND @overwrite = 0
            BEGIN
                RETURN;
        END;
        SELECT @strErrorMessage = 'opening the File System Object';
        EXECUTE @hr = [sp_OACreate] 
                'Scripting.FileSystemObject', 
                @objFileSystem OUT;
        IF @hR = 0
            BEGIN
                SELECT @objErrorObject = @objFileSystem, 
                       @strErrorMessage = 'Creating file "' + @fileAndPath + '"';
        END;
        IF @hR = 0
            BEGIN
                EXECUTE @hr = [sp_OAMethod] 
                        @objFileSystem, 
                        'CreateTextFile', 
                        @objTextStream OUT, 
                        @fileAndPath, 
                        2, 
                        [False];
        END;
        IF @hR = 0
            BEGIN
                SELECT @objErrorObject = @objTextStream, 
                       @strErrorMessage = 'writing to the file "' + @fileAndPath + '"';
        END;
        IF @hR = 0
            BEGIN
                EXECUTE @hr = [sp_OAMethod] 
                        @objTextStream, 
                        'Write', 
                        NULL, 
                        @string;
        END;
        IF @hR = 0
            BEGIN
                SELECT @objErrorObject = @objTextStream, 
                       @strErrorMessage = 'closing the file "' + @fileAndPath + '"';
        END;
        IF @hR = 0
            BEGIN
                EXECUTE @hr = [sp_OAMethod] 
                        @objTextStream, 
                        'Close';
        END;
        IF @hr <> 0
            BEGIN
                DECLARE @source VARCHAR(255), @description VARCHAR(255), @helpfile VARCHAR(255), @helpID INT;
                EXECUTE [sp_OAGetErrorInfo] 
                        @objErrorObject, 
                        @source OUTPUT, 
                        @description OUTPUT, 
                        @helpfile OUTPUT, 
                        @helpID OUTPUT;
                SELECT @strErrorMessage = 'Error whilst ' + COALESCE(@strErrorMessage, 'doing something') + ', ' + COALESCE(@description, '');
                RAISERROR(@strErrorMessage, 16, 1);
        END;
        EXECUTE [sp_OADestroy] 
                @objTextStream;
        EXECUTE [sp_OADestroy] 
                @objFileSystem;
    END;
GO 
PRINT '\etl.ap_InsertETLlog.StoredProcedure.sql' 
GO 
 
IF OBJECT_ID('[etl].[ap_InsertETLlog]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_InsertETLlog];
GO

CREATE PROCEDURE [etl].[ap_InsertETLlog] 
                 @type        TINYINT, 
                 @logtime     DATETIME OUTPUT, 
                 @rowcount    INT, 
                 @procname    VARCHAR(128), 
                 @source      VARCHAR(128), 
                 @destination VARCHAR(128), 
                 @message     VARCHAR(2048), 
                 @logXML      XML           = NULL
AS
    BEGIN
        DECLARE @starttime DATETIME, @endtime DATETIME, @nestlevel INT;
        SELECT @starttime = @logtime, 
               @endtime = GETDATE(), 
               @nestlevel = @@nestLevel - 1;
        INSERT INTO [etl].[log]
        ([type], 
         [starttime], 
         [endtime], 
         [rowcount], 
         [spid], 
         [username], 
         [procname], 
         [nestlevel], 
         [source], 
         [destination], 
         [message]
        )
        VALUES
        (@type, 
         @starttime, 
         @endtime, 
         @rowcount, 
         @@spid, 
         SYSTEM_USER, 
         @procname, 
         @nestlevel, 
         @source, 
         @destination, 
         @message
        );
        IF @logXML IS NOT NULL
            BEGIN
                INSERT INTO [etl].[logXML]
                VALUES
                (@@identity, 
                 @logXML
                );
        END;
        SELECT @logtime = @endtime;
    END;
GO 
PRINT '\etl.ap_GenericPopulate.StoredProcedure.sql' 
GO 
 
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
PRINT '\etl.ap_GetFirstRowFromFile.StoredProcedure.sql' 
GO 
 
IF OBJECT_ID('[etl].[ap_GetFirstRowFromFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetFirstRowFromFile];
GO

CREATE PROCEDURE [etl].[ap_GetFirstRowFromFile] 
                 @path            VARCHAR(260), 
                 @filename        VARCHAR(260), 
                 @codepage        VARCHAR(10)  = '65001', 
                 @rowTerminator   VARCHAR(6)   = '0x0d0a', 
                 @fieldterminator CHAR(1)      = ',', 
                 @firstRow        VARCHAR(MAX) OUTPUT, 
                 @debug           BIT          = 0
AS
    BEGIN
        DECLARE @sqlCmd VARCHAR(MAX), @xml AS XML, @dedupedcols VARCHAR(MAX)= '';
        CREATE TABLE [#IMPORT](
                     [FIRSTROW] NVARCHAR(MAX)
        );
        SET @sqlCmd = 'BULK INSERT #IMPORT FROM ''' + @path + '\' + @filename + ''' WITH(CODEPAGE = ''' + @codepage + ''', DATAFILETYPE = ''char'', FIRSTROW = 1, LASTROW = 1, FIELDTERMINATOR = ''\0'', ROWTERMINATOR = ''' + @rowTerminator + ''');';
        IF @debug = 1
            BEGIN
                PRINT 'CREATE TABLE [#IMPORT]([FIRSTROW] NVARCHAR(MAX));';
                PRINT @sqlCmd;
        END;
        EXEC (@sqlCmd);
        SELECT @firstRow = [FIRSTROW]
          FROM [#IMPORT];
        DROP TABLE [#IMPORT];
        SELECT @firstRow = REPLACE(@firstRow, '&', '&amp;');
        IF @debug = 1
            BEGIN
                PRINT 'DROP TABLE [#IMPORT];';
        END;
        SET @xml = CAST(('<X>' + replace(@firstrow, @fieldterminator, '</X><X>') + '</X>') AS XML);
        WITH colnames
             AS (SELECT [N].value
                        ('.', 'varchar(128)') AS value, 
                        [colseq] = ROW_NUMBER() OVER(
                        ORDER BY (
                                 SELECT 1
                                 ) )
                   FROM @xml.nodes
                        ('X') AS [T]([N])),
             colnameswithdup
             AS (SELECT *, 
                        [dup] = ROW_NUMBER() OVER(PARTITION BY value
                        ORDER BY (
                                 SELECT 1
                                 ) )
                   FROM [colnames]),
             uniquecolnames
             AS (SELECT *, 
                        [uniquecolname] = [value] + CASE
                                                        WHEN [dup] = 1 THEN ''
                                                        ELSE '_' + CAST([dup] - 1 AS VARCHAR(3))
                                                    END
                   FROM [colnameswithdup])
             SELECT @dedupedcols = @dedupedcols + @fieldterminator + [uniquecolname]
               FROM [UniqueColNames]
             ORDER BY [colseq];
        SELECT @firstRow = STUFF(@dedupedcols, 1, LEN(@fieldterminator), '');
        RETURN;
    END;
GO 
PRINT '\etl.ap_ImportFiles.StoredProcedure.sql' 
GO 
 
IF OBJECT_ID('[etl].[ap_ImportFiles]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_ImportFiles];
GO

CREATE PROCEDURE [etl].[ap_ImportFiles] 
                 @rootfolder         VARCHAR(260) = NULL, 
                 @singlefolder       VARCHAR(260) = NULL, 
                 @importschema       SYSNAME      = 'EXTRACTS', 
                 @stagingschema      SYSNAME      = 'TRANSFORMS', 
                 @filemask           VARCHAR(260) = NULL, 
                 @fieldterminator    CHAR(1)      = ',', 
                 @rowterminator      VARCHAR(6)   = '0x0d0a', 
                 @textdelimiter      CHAR(1)      = '"', 
                 @codepage           VARCHAR(10)  = '65001', 
                 @drop               BIT          = 0, 
                 @override           BIT          = 0, 
                 @overridetable      SYSNAME      = NULL, 
                 @populatestaging    BIT          = 1, 
                 @truncatestaging    TINYINT      = 0, -- 0=no truncate, 1=truncate if file found, 2=always truncate
                 @archive            TINYINT      = 1, -- 1=move, 2=replace
                 @archiveFolder      VARCHAR(260) = 'Completed', 
                 @overwriteschemaini BIT          = 1, 
                 @deletefile         BIT          = 0, 
                 @convertnull        BIT          = 0, 
                 @rtrim              BIT          = 0, 
                 @header             BIT          = 1, 
                 @matchon            VARCHAR(10)  = 'NAME', 
                 @skiprows           INT          = 0, 
                 @memo               BIT          = 0, -- 0=text, 1=memo
                 @debug              BIT          = 0, 
                 @filecount          INT          = 0 OUTPUT, 
                 @recordcount        INT          = 0 OUTPUT
WITH RECOMPILE
AS
    BEGIN
        SET NOCOUNT ON;
        DECLARE @folders TABLE(
                               [rownumber]  INT IDENTITY(1, 1), 
                               [folderName] VARCHAR(260), 
                               [depth]      INT
        );
        DECLARE @files TABLE(
                             [rownumber] INT IDENTITY(1, 1), 
                             [fileName]  VARCHAR(260), 
                             [depth]     INT, 
                             [isfile]    BIT
        );
        DECLARE @cmd VARCHAR(8000), @firstrow NVARCHAR(MAX), @rownumber INT, @numfiles INT, @currfile INT, @foldername VARCHAR(260), @filename VARCHAR(260), @path VARCHAR(260), @tablename SYSNAME, @truncate_sql VARCHAR(MAX), @sqlcmd VARCHAR(MAX), @inifilecontent VARCHAR(MAX), @fullfoldername VARCHAR(1000), @xml AS XML;
        DECLARE @crlf CHAR(2);
        SET @crlf = CHAR(13) + CHAR(10);
        DECLARE @rc INT, @type TINYINT, @logtime DATETIME, @rowcount INT, @procname VARCHAR(128), @source VARCHAR(128), @destination VARCHAR(128), @message VARCHAR(2048), @logxml XML;
        DECLARE @srcFile VARCHAR(260), @dstFile VARCHAR(260), @newFirstRow VARCHAR(MAX);
        DECLARE @tmpFile VARCHAR(260);
        DECLARE @dest_table SYSNAME;
        DECLARE @tbl_Suppress_Results AS TABLE(
                                               [output] VARCHAR(MAX)
        );
        SET @tmpFile = '~import.tmp';
        SET @filecount = 0;
        SET @recordcount = 0;
        IF @debug = 1
            BEGIN
                PRINT '-- Running in debug mode - no data changed';
        END;
        SELECT @type = 0, 
               @logtime = GETDATE(), 
               @procname = OBJECT_NAME(@@procid), 
               @source = NULL, 
               @destination = NULL, 
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
        -- Get list of folders at the root
        --
        INSERT INTO @folders
               ([folderName], 
                [depth]
               ) 
        EXEC [Master].[dbo].[xp_DirTree] 
             @rootfolder, 
             1, 
             0;
        --
        -- For each loop on joined folders and tables
        --
        IF @override = 0
            BEGIN
                DECLARE csrFolders CURSOR
                FOR SELECT [F].[rownumber], 
                           [F].[folderName]
                      FROM @folders AS [F]
                           INNER JOIN [INFORMATION_SCHEMA].[TABLES] AS [T] ON [T].[TABLE_SCHEMA] = @stagingschema
                                                                              AND [F].[folderName] = [T].[TABLE_NAME]
                                                                              AND [F].[folderName] = COALESCE(@singlefolder, [F].[folderName]);
        END;
            ELSE
            BEGIN
                DECLARE csrFolders CURSOR
                FOR SELECT [F].[rownumber], 
                           [F].[folderName]
                      FROM @folders AS [F]
                     WHERE [F].[folderName] = COALESCE(@singleFolder, [F].[folderName]);
        END;
        OPEN csrFolders;
        FETCH NEXT FROM csrFolders INTO @rownumber, @foldername;
        WHILE @@fetch_status = 0
            BEGIN
                SELECT @type = 0, 
                       @logtime = GETDATE(), 
                       @source = NULL, 
                       @destination = NULL, 
                       @message = 'Processing folder ' + @foldername;
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
                SELECT @fullfoldername = @rootfolder + '\' + @foldername;
                DELETE @files;
                INSERT INTO @files
                       ([fileName], 
                        [depth], 
                        [isfile]
                       ) 
                EXEC [master]..[xp_dirtree] 
                     @fullfoldername, 
                     1, 
                     1;
                DECLARE csrFiles CURSOR
                FOR SELECT [F].[rownumber], 
                           [F].[fileName]
                      FROM @files AS [F]
                     WHERE [isfile] = 1
                           AND (@filemask IS NULL
                                OR [fileName] LIKE @filemask);
                OPEN csrFiles;
                SELECT @numfiles = COUNT(*), 
                       @currfile = 0
                  FROM @files AS [F]
                 WHERE [isfile] = 1
                       AND (@filemask IS NULL
                            OR [fileName] LIKE @filemask);
                SELECT @filecount = @numfiles;
                FETCH NEXT FROM csrFiles INTO @rownumber, @filename;
                IF(@@fetch_status = 0
                   AND @truncatestaging = 1)
                  OR (@truncatestaging = 2)
                    BEGIN
                        SET @truncate_sql = 'TRUNCATE TABLE ' + QUOTENAME(@stagingschema) + '.' + QUOTENAME(COALESCE(@overridetable, @foldername));
                        IF @debug = 1
                            PRINT @truncate_sql;
                            ELSE
                            EXEC (@truncate_sql);
                END;
                WHILE @@fetch_status = 0
                    BEGIN
                        SELECT @tablename = REVERSE(STUFF(REVERSE(@filename), 1, CHARINDEX('.', REVERSE(@filename)), '')), 
                               @currfile = @currfile + 1;
                        SELECT @type = 0, 
                               @logtime = GETDATE(), 
                               @source = @filename, 
                               @destination = QUOTENAME(@importschema) + '.' + QUOTENAME(@tablename), 
                               @message = 'Processing file ' + LTRIM(STR(@currfile)) + ' of ' + LTRIM(STR(@numfiles)) + ' : ' + @filename;
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
                        SET @path = @rootfolder + '\' + @foldername;
                        EXEC [etl].[ap_GetFirstRowFromFile] 
                             @path = @path, 
                             @filename = @filename, 
                             @codepage = @codepage, 
                             @rowterminator = @rowterminator, 
                             @fieldterminator = @fieldterminator, 
                             @firstrow = @firstrow OUTPUT, 
                             @debug = @debug;
                        WHILE PATINDEX('%"%"%', @firstrow) > 0
                            BEGIN
                                SELECT @firstrow = REPLACE(@firstrow, SUBSTRING(@firstrow, CHARINDEX('"', @firstrow), CHARINDEX('"', @firstrow, 1 + CHARINDEX('"', @firstrow)) - CHARINDEX('"', @firstrow) + 1), REPLACE(REPLACE(SUBSTRING(@firstrow, CHARINDEX('"', @firstrow), CHARINDEX('"', @firstrow, 1 + CHARINDEX('"', @firstrow)) - CHARINDEX('"', @firstrow) + 1), @fieldterminator, ''), '"', ''));
                            END;
                        IF @debug = 1
                            PRINT '-- ' + @firstrow;
                        SELECT @inifilecontent = '[' + @tmpFile + ']' + @crlf + 'TextDelimiter=' + COALESCE(NULLIF(@textdelimiter, ''), 'None') + @crlf + 'Format=' + CASE
                                                                                                                                                                          WHEN @fieldterminator = CHAR(9) THEN 'TabDelimited'
                                                                                                                                                                          WHEN @fieldterminator = ',' THEN 'CSVDelimited'
                                                                                                                                                                          ELSE 'Delimited(' + @fieldterminator + ')'
                                                                                                                                                                      END + @crlf + 'ColNameHeader=' + CASE
                                                                                                                                                                                                           WHEN @header = 1 THEN 'True'
                                                                                                                                                                                                           ELSE 'False'
                                                                                                                                                                                                       END + @crlf;
                        SELECT @newFirstRow = '';
                        IF @header = 0
                            BEGIN
                                SELECT @firstrow = REPLICATE(@fieldterminator, LEN(@firstrow) - LEN(REPLACE(@firstrow, @fieldterminator, '')));
                        END;
                        SELECT @firstRow = REPLACE(@firstRow, '&', '&amp;');
                        SET @xml = CAST(('<X>' + replace(@firstrow, @fieldterminator, '</X><X>') + '</X>') AS XML);
                        WITH colnames
                             AS (SELECT [N].value
                                        ('.', 'varchar(128)') AS value
                                   FROM @xml.nodes
                                        ('X') AS [T]([N]))
                             SELECT @inifilecontent = @inifilecontent + 'Col' + LTRIM(STR([ColNumber])) + '="' + CASE
                                                                                                                     WHEN @header = 1 THEN [ColName]
                                                                                                                     ELSE 'F' + LTRIM(STR([ColNumber]))
                                                                                                                 END + '" ' + CASE
                                                                                                                                  WHEN @memo = 1 THEN 'memo'
                                                                                                                                  ELSE 'text'
                                                                                                                              END + @crlf, 
                                    @newFirstRow = @newFirstRow + CASE
                                                                      WHEN @header = 1 THEN @fieldterminator + [ColName]
                                                                      ELSE @fieldterminator + 'F' + LTRIM(STR([ColNumber]))
                                                                  END
                               FROM (
                                    SELECT value AS        [ColName], 
                                           ROW_NUMBER() OVER(
                                           ORDER BY (
                                                    SELECT 1
                                                    ) ) AS [ColNumber]
                                      FROM [colnames]
                                    ) AS [t];
                        SELECT @firstrow = STUFF(@newFirstRow, 1, 1, '');
                        IF @debug = 1
                            BEGIN
                                PRINT '/*' + @crlf + @inifilecontent + @crlf + '*/';
                        END;
                            ELSE
                            BEGIN
                                EXEC [etl].[ap_WriteStringToFile] 
                                     @string = @iniFileContent, 
                                     @path = @path, 
                                     @filename = 'schema.ini', 
                                     @overwrite = @overwriteSchemaIni;
                        END;
                        IF @debug = 1
                            PRINT '-- [HDR]=' + @firstrow;
                        SELECT @sqlcmd = 'IF OBJECT_ID(''' + QUOTENAME(@importschema) + '.' + QUOTENAME(@tablename) + ''', ''U'') IS NOT NULL DROP TABLE ' + QUOTENAME(@importschema) + '.' + QUOTENAME(@tablename) + '; CREATE TABLE ' + QUOTENAME(@importschema) + '.' + QUOTENAME(@tablename) + '([FILENAME] VARCHAR(260) NOT NULL, [ROWNUMBER] INT NOT NULL, [' + REPLACE(UPPER(@firstrow), @fieldterminator, '] nvarchar(max), [') + '] nvarchar(max))';
                        IF @debug = 1
                            BEGIN
                                SELECT CAST('<root><![CDATA[' + @sqlcmd + ']]></root>' AS XML);
                        END;
                            ELSE
                            BEGIN
                                EXEC (@sqlCmd);
                        END;
                        SELECT @srcFile = @path + '\' + @filename;
                        SELECT @dstFile = @path + '\' + @tmpFile;
                        INSERT INTO @tbl_Suppress_Results
                               ([output]
                               ) 
                               SELECT [etl].[FileCopy]
                                      (@srcFile, @dstFile, 1);
                        SELECT @sqlCmd = ';WITH CTE AS (SELECT ''' + @fileName + ''' AS [FILENAME], ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS [ROWNUMBER], * FROM OPENROWSET(''Microsoft.ACE.OLEDB.12.0'', ''Text;Database=' + @rootFolder + '\' + @folderName + ';HDR=' + CASE
                                                                                                                                                                                                                                                                               WHEN @header = 1 THEN 'Yes'
                                                                                                                                                                                                                                                                               ELSE 'No'
                                                                                                                                                                                                                                                                           END + ';CharacterSet=' + @codepage + ';IMEX=1'', ''SELECT * FROM [' + @tmpFile + ']'')) INSERT INTO ' + QUOTENAME(@importSchema) + '.' + QUOTENAME(@tableName) + ' SELECT * FROM [cte] WHERE [ROWNUMBER] > ' + LTRIM(STR(@skiprows));
                        IF @debug = 1
                            BEGIN
                                SELECT CAST('<root><![CDATA[' + @sqlcmd + ']]></root>' AS XML);
                        END;
                            ELSE
                            BEGIN
                                EXEC (@sqlCmd);
                                SELECT @rowcount = @@rowCount, 
                                       @message = 'Inserted ' + LTRIM(STR(@rowcount)) + ' records into ' + QUOTENAME(@importSchema) + '.' + QUOTENAME(@tableName);
                                SELECT @recordcount = ISNULL(@recordcount, 0) + @rowcount;
                                IF @debug = 0
                                    BEGIN
                                        EXECUTE @rC = [etl].[ap_InsertETLlog] 
                                                @type, 
                                                @logtime OUTPUT, 
                                                @rowcount, 
                                                @procname, 
                                                @source, 
                                                @destination, 
                                                @message;
                                END;
                                PRINT @message;
                        END;
                        INSERT INTO @tbl_Suppress_Results
                               ([output]
                               ) 
                               SELECT [etl].[FileDelete]
                                      (@dstFile);
                        IF @populatestaging = 1
                            BEGIN
                                SET @dest_table = @foldername;
                                IF @overridetable IS NOT NULL
                                    SET @dest_table = @overridetable;
                                EXEC [etl].[ap_GenericPopulate] 
                                     @source_table = @tablename, 
                                     @dest_table = @dest_table, 
                                     @source_schema = @importschema, 
                                     @dest_schema = @stagingschema, 
                                     @matchon = @matchon, 
                                     @convertnull = @convertnull, 
                                     @rtrim = @rtrim, 
                                     @debug = @debug;
                        END;
                        IF @drop = 1
                            BEGIN
                                SELECT @sqlcmd = 'IF OBJECT_ID(''' + QUOTENAME(@importschema) + '.' + QUOTENAME(@tablename) + ''', ''U'') IS NOT NULL DROP TABLE ' + QUOTENAME(@importschema) + '.' + QUOTENAME(@tablename) + ';';
                                IF @debug = 1
                                    BEGIN
                                        PRINT @sqlcmd;
                                END;
                                    ELSE
                                    BEGIN
                                        EXEC (@sqlCmd);
                                END;
                        END;
                        IF @deletefile = 1
                            BEGIN
                                SELECT @srcFile = @path + '\' + @filename;
                                IF @debug = 1
                                    BEGIN
                                        PRINT 'DEL ' + @srcFile;
                                END;
                                    ELSE
                                    BEGIN
                                        INSERT INTO @tbl_Suppress_Results
                                               ([output]
                                               ) 
                                               SELECT [etl].[FileDelete]
                                                      (@srcFile);
                                END;
                        END;
                        IF @archive = 1
                            BEGIN
                                SELECT @srcFile = @path + '\' + @filename;
                                SELECT @dstFile = @path + '\' + @archiveFolder + '\' + @filename;
                                IF @debug = 1
                                    BEGIN
                                        PRINT 'MOVE "' + @srcFile + '" "' + @dstFile + '"';
                                END;
                                    ELSE
                                    BEGIN
                                        INSERT INTO @tbl_Suppress_Results
                                               ([output]
                                               ) 
                                               SELECT [etl].[FileMove]
                                                      (@srcFile, @dstFile);
                                END;
                        END;
                        IF @archive = 2
                            BEGIN
                                SELECT @srcFile = @path + '\' + @filename;
                                SELECT @dstFile = @path + '\' + @archiveFolder + '\' + @filename;
                                IF @debug = 1
                                    BEGIN
                                        PRINT 'REPLACE "' + @srcFile + '" "' + @dstFile + '"';
                                END;
                                    ELSE
                                    BEGIN
                                        INSERT INTO @tbl_Suppress_Results
                                               ([output]
                                               ) 
                                               SELECT [etl].[FileReplace]
                                                      (@srcFile, @dstFile, NULL, 1);
                                END;
                        END;
                        SELECT @rowcount = NULL, 
                               @type = 0, 
                               @message = 'Finished file';
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
                        FETCH NEXT FROM csrFiles INTO @rownumber, @filename;
                    END;
                CLOSE csrFiles;
                DEALLOCATE csrFiles;
                IF @numfiles = 0
                    BEGIN
                        SELECT @type = 0, 
                               @logtime = GETDATE(), 
                               @source = NULL, 
                               @destination = NULL, 
                               @message = 'No files found in folder ' + @foldername;
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
                SELECT @type = 0, 
                       @logtime = GETDATE(), 
                       @source = NULL, 
                       @destination = NULL, 
                       @message = 'Finished folder ' + @foldername;
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
                FETCH NEXT FROM csrFolders INTO @rownumber, @foldername;
            END;
        CLOSE csrFolders;
        DEALLOCATE csrFolders;
        SELECT @rowcount = NULL, 
               @source = NULL, 
               @destination = NULL, 
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
        SET NOCOUNT OFF;
        RETURN;
    END;
GO 
PRINT '\etl.ap_ImportExcel.StoredProcedure.sql' 
GO 
 

IF OBJECT_ID('[etl].[ap_ImportExcel]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_ImportExcel];
GO
CREATE PROCEDURE [etl].[ap_ImportExcel] @rootfolder      VARCHAR(260) = NULL, 
                                        @singlefolder    VARCHAR(260) = NULL, 
                                        @importschema    SYSNAME      = 'EXTRACTS', 
                                        @stagingschema   SYSNAME      = 'TRANSFORMS', 
                                        @fileextension   VARCHAR(260) = 'xls', 
                                        @drop            BIT          = 0, 
                                        @override        BIT          = 0, 
                                        @overridetable   SYSNAME      = NULL, 
                                        @populatestaging BIT          = 1, 
                                        @truncatestaging BIT          = 0, 
                                        @archive         TINYINT      = 1, -- 1=move, 2=replace
                                        @archiveFolder   VARCHAR(260) = 'Completed', 
                                        @deletefile      BIT          = 0, 
                                        @convertnull     BIT          = 0, 
                                        @rtrim           BIT          = 0, 
                                        @header          BIT          = 1, 
                                        @matchon         VARCHAR(10)  = 'NAME', -- (NAME or POSITION)
                                        @skiprows        INT          = 0, 
                                        @namedranges     TINYINT      = 0, -- (0=None, 1=Only, Else=All)
                                        @rangename       SYSNAME      = NULL, -- (Single SheetName or NamedRange)
                                        @debug           BIT          = 0, 
                                        @filecount       INT          = 0 OUTPUT, 
                                        @recordcount     INT          = 0 OUTPUT
WITH RECOMPILE
AS
     SET NOCOUNT ON;
     DECLARE @folders TABLE([rownumber]  INT IDENTITY(1, 1), 
                            [folderName] VARCHAR(260), 
                            [depth]      INT
     );
     DECLARE @files TABLE([rownumber] INT IDENTITY(1, 1), 
                          [fileName]  VARCHAR(260), 
                          [depth]     INT, 
                          [isfile]    BIT
     );
     DECLARE @rownumber      INT, 
             @numfiles       INT, 
             @currfile       INT, 
             @foldername     VARCHAR(260), 
             @filename       VARCHAR(260), 
             @path           VARCHAR(260), 
             @tablename      SYSNAME, 
             @truncate_sql   VARCHAR(MAX), 
             @sqlCmd         VARCHAR(MAX), 
             @fullfoldername VARCHAR(1000);
     DECLARE @rc          INT, 
             @type        TINYINT, 
             @logtime     DATETIME, 
             @rowcount    INT, 
             @procname    VARCHAR(128), 
             @source      VARCHAR(128), 
             @destination VARCHAR(128), 
             @message     VARCHAR(2048), 
             @logxml      XML;
     DECLARE @srcFile VARCHAR(260), 
             @dstFile VARCHAR(260);
     DECLARE @linkedServerName SYSNAME;
     SET @linkedServerName = 'TempExcelSpreadsheet';
     DECLARE @sheetName VARCHAR(255);
     DECLARE @sheetNameNoQuote VARCHAR(255);
     DECLARE @basename VARCHAR(1000);
     DECLARE @datasrc VARCHAR(1000);
     DECLARE @provstr VARCHAR(1000);
     DECLARE @source_table SYSNAME;
     DECLARE @dest_table SYSNAME;
     DECLARE @tbl_Suppress_Results AS TABLE([output] VARCHAR(MAX));
     SET @filecount = 0;
     SET @recordcount = 0;
     IF @debug = 1
         BEGIN
             PRINT '-- Running in debug mode - no data changed';
     END;
     SELECT @type = 0, 
            @logtime = GETDATE(), 
            @procname = OBJECT_NAME(@@procid), 
            @source = NULL, 
            @destination = NULL, 
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
     -- Get list of folders at the root
     --
     INSERT INTO @folders ([folderName], 
                           [depth]) 
     EXEC [Master].[dbo].[xp_DirTree] 
          @rootfolder, 
          1, 
          0;
     --
     -- For each loop on joined folders and tables
     --
     IF @override = 0
         BEGIN
             DECLARE csrFolders CURSOR
             FOR SELECT [F].[rownumber], 
                        [F].[folderName]
                   FROM @folders AS [F]
                        INNER JOIN [INFORMATION_SCHEMA].[TABLES] AS [T] ON [T].[TABLE_SCHEMA] = @stagingschema
                                                                           AND [F].[folderName] = [T].[TABLE_NAME]
                                                                           AND [F].[folderName] = COALESCE(@singlefolder, [F].[folderName]);
     END;
         ELSE
         BEGIN
             DECLARE csrFolders CURSOR
             FOR SELECT [F].[rownumber], 
                        [F].[folderName]
                   FROM @folders AS [F]
                  WHERE [F].[folderName] = COALESCE(@singleFolder, [F].[folderName]);
     END;
     OPEN csrFolders;
     FETCH NEXT FROM csrFolders INTO @rownumber, 
                                     @foldername;
     WHILE @@fetch_status = 0
         BEGIN
             SELECT @type = 0, 
                    @logtime = GETDATE(), 
                    @source = NULL, 
                    @destination = NULL, 
                    @message = 'Processing folder ' + @foldername;
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
             SELECT @fullfoldername = @rootfolder + '\' + @foldername;
             DELETE @files;
             INSERT INTO @files ([fileName], 
                                 [depth], 
                                 [isfile]) 
             EXEC [master]..[xp_dirtree] 
                  @fullfoldername, 
                  1, 
                  1;
             DECLARE csrFiles CURSOR
             FOR SELECT [F].[rownumber], 
                        [F].[fileName]
                   FROM @files AS [F]
                  WHERE [fileName] LIKE '%[.]' + @fileextension
                        AND [isfile] = 1;
             OPEN csrFiles;
             SELECT @numfiles = COUNT(*), 
                    @currfile = 0
               FROM @files AS [F]
              WHERE [fileName] LIKE '%[.]' + @fileextension
                    AND [isfile] = 1;
             SELECT @filecount = @numfiles;
             FETCH NEXT FROM csrFiles INTO @rownumber, 
                                           @filename;
             IF @@fetch_status = 0
                AND @truncatestaging = 1
                 BEGIN
                     SET @truncate_sql = 'TRUNCATE TABLE ' + QUOTENAME(@stagingschema) + '.' + QUOTENAME(COALESCE(@overridetable, @foldername));
                     IF @debug = 1
                         PRINT @truncate_sql;
                         ELSE
                         EXEC (@truncate_sql);
             END;
             WHILE @@fetch_status = 0
                 BEGIN
                     SELECT @basename = REVERSE(STUFF(REVERSE(@filename), 1, CHARINDEX('.', REVERSE(@filename)), '')), 
                            @currfile = @currfile + 1;
                     SELECT @type = 0, 
                            @logtime = GETDATE(), 
                            @source = @filename, 
                            @destination = QUOTENAME(@importschema) + '.' + QUOTENAME(@basename), 
                            @message = 'Processing file ' + LTRIM(STR(@currfile)) + ' of ' + LTRIM(STR(@numfiles)) + ' : ' + @filename;
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
                     -- Remove existing linked server (if necessary)
                     --
                     IF EXISTS (
                               SELECT NULL
                                 FROM [sys].[servers]
                                WHERE [name] = @linkedServerName
                               ) 
                         BEGIN
                             EXEC [sp_dropserver] 
                                  @server = @linkedServerName, 
                                  @droplogins = 'droplogins';
                     END;
                     --
                     -- Add the linked server
                     --
                     SET @path = @rootfolder + '\' + @foldername;
                     SELECT @datasrc = @path + '\' + @filename;
                     SELECT @provstr = 'Excel 12.0;HDR=' + CASE
                                                               WHEN @header = 0 THEN 'No'
                                                               ELSE 'Yes'
                                                           END + ';IMEX=1';
                     EXEC [sp_addlinkedserver] 
                          @server = @linkedServerName, 
                          @srvproduct = 'ACE 12.0', 
                          @provider = 'Microsoft.ACE.OLEDB.12.0', 
                          @datasrc = @datasrc, 
                          @provstr = @provstr;
                     --
                     -- Grab the current user to use as a remote login
                     --
                     DECLARE @suser_sname NVARCHAR(256);
                     SET @suser_sname = SUSER_SNAME();
                     --
                     -- Add the current user as a login
                     --
                     EXEC [SP_ADDLINKEDSRVLOGIN] 
                          @rmtsrvname = @linkedServerName, 
                          @useself = 'false', 
                          @locallogin = @suser_sname, 
                          @rmtuser = NULL, 
                          @rmtpassword = NULL;
                     --
                     -- Return the table info, each worksheet gets its own unique name
                     --
                     IF OBJECT_ID('tempdb..#MyTempTable') IS NOT NULL
                         DROP TABLE [#MyTempTable];
                     SELECT *
                     INTO [#MyTempTable]
                       FROM OPENROWSET('SQLNCLI', 'Server=(local);Trusted_Connection=yes;', 'EXEC sp_tables_ex TempExcelSpreadsheet');
                     IF @namedranges = 0
                         BEGIN
                             DELETE FROM [#MyTempTable]
                              WHERE REPLACE([TABLE_NAME], '''', '') NOT LIKE '%[$]';
                     END;
                     IF @namedranges = 1
                         BEGIN
                             DELETE FROM [#MyTempTable]
                              WHERE REPLACE([TABLE_NAME], '''', '') LIKE '%[$]';
                     END;
                     IF @rangename IS NOT NULL
                         BEGIN
                             DELETE FROM [#MyTempTable]
                              WHERE REPLACE([TABLE_NAME], '''', '') <> @rangename;
                     END;
                     SELECT @sheetName = MIN([TABLE_NAME])
                       FROM [#MyTempTable];
                     WHILE @sheetName IS NOT NULL
                         BEGIN
                             SELECT @sheetNameNoQuote = replace(@sheetName, '''', '');
                             SELECT @tablename = QUOTENAME(@importSchema) + '.' + QUOTENAME(@basename + '_' + @sheetNameNoQuote);
                             SELECT @sqlCmd = N'IF OBJECT_ID(''' + @tablename + ''', ''U'') IS NOT NULL DROP TABLE ' + @tablename + ';';
                             IF @debug = 1
                                 BEGIN
                                     PRINT @sqlcmd;
                             END;
                                 ELSE
                                 BEGIN
                                     EXEC (@sqlCmd);
                             END;
                             SELECT @sqlCmd = ';WITH CTE AS (SELECT CAST(''' + @basename + '_' + @sheetNameNoQuote + ''' AS NVARCHAR(255)) AS [FILENAME], ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS [ROWNUMBER], * FROM OPENROWSET(''Microsoft.ACE.OLEDB.12.0'', ''Excel 12.0; Database=' + @datasrc + ';HDR=' + CASE
                                                                                                                                                                                                                                                                                                                     WHEN @header = 0 THEN 'No'
                                                                                                                                                                                                                                                                                                                     ELSE 'Yes'
                                                                                                                                                                                                                                                                                                                 END + ';IMEX=1'', ''SELECT * FROM [' + @sheetNameNoQuote + '];'')) SELECT * INTO ' + @tablename + ' FROM cte WHERE ROWNUMBER > ' + LTRIM(STR(@skiprows));
                             IF @debug = 1
                                 BEGIN
                                     PRINT @sqlCmd;
                             END;
                                 ELSE
                                 BEGIN
                                     EXEC (@sqlCmd);
                                     SELECT @rowcount = @@rowCount, 
                                            @message = 'Inserted ' + LTRIM(STR(@rowcount)) + ' records into ' + @tableName;
                                     SELECT @recordcount = ISNULL(@recordcount, 0) + @rowcount;
                                     IF @debug = 0
                                         BEGIN
                                             EXECUTE @rC = [etl].[ap_InsertETLlog] 
                                                     @type, 
                                                     @logtime OUTPUT, 
                                                     @rowcount, 
                                                     @procname, 
                                                     @source, 
                                                     @destination, 
                                                     @message;
                                     END;
                                     PRINT @message;
                             END;
                             IF @populatestaging = 1
                                 BEGIN
                                     SELECT @source_table = @basename + '_' + @sheetNameNoQuote;
                                     SET @dest_table = @foldername;
                                     IF @overridetable IS NOT NULL
                                         SET @dest_table = @overridetable;
                                     EXEC [etl].[ap_GenericPopulate] 
                                          @source_table = @source_table, 
                                          @dest_table = @dest_table, 
                                          @source_schema = @importschema, 
                                          @dest_schema = @stagingschema, 
                                          @matchon = @matchon, 
                                          @convertnull = @convertnull, 
                                          @rtrim = @rtrim, 
                                          @debug = @debug;
                             END;
                             IF @drop = 1
                                 BEGIN
                                     SELECT @sqlCmd = 'IF OBJECT_ID(''' + @tablename + ''', ''U'') IS NOT NULL DROP TABLE ' + @tablename + ';';
                                     IF @debug = 1
                                         BEGIN
                                             PRINT @sqlCmd;
                                     END;
                                         ELSE
                                         BEGIN
                                             EXEC (@sqlCmd);
                                     END;
                             END;
                             SELECT @sheetName = MIN([TABLE_NAME])
                               FROM [#MyTempTable]
                              WHERE [TABLE_NAME] > @sheetName;
                         END;
                     --
                     -- delete file
                     --
                     IF @deletefile = 1
                         BEGIN
                             SELECT @srcFile = @path + '\' + @filename;
                             IF @debug = 1
                                 BEGIN
                                     PRINT 'DEL ' + @srcFile;
                             END;
                                 ELSE
                                 BEGIN
                                     INSERT INTO @tbl_Suppress_Results ([output]) 
                                     SELECT [etl].[FileDelete] (@srcFile);
                             END;
                     END;
                     --
                     -- archive file
                     --
                     IF @archive = 1
                         BEGIN
                             SELECT @srcFile = @path + '\' + @filename;
                             SELECT @dstFile = @path + '\' + @archiveFolder + '\' + @filename;
                             IF @debug = 1
                                 BEGIN
                                     PRINT 'MOVE "' + @srcFile + '" "' + @dstFile + '"';
                             END;
                                 ELSE
                                 BEGIN
                                     INSERT INTO @tbl_Suppress_Results ([output]) 
                                     SELECT [etl].[FileMove] (@srcFile, @dstFile);
                             END;
                     END;
                     IF @archive = 2
                         BEGIN
                             SELECT @srcFile = @path + '\' + @filename;
                             SELECT @dstFile = @path + '\' + @archiveFolder + '\' + @filename;
                             IF @debug = 1
                                 BEGIN
                                     PRINT 'REPLACE "' + @srcFile + '" "' + @dstFile + '"';
                             END;
                                 ELSE
                                 BEGIN
                                     INSERT INTO @tbl_Suppress_Results ([output]) 
                                     SELECT [etl].[FileReplace] (@srcFile, @dstFile, NULL, 1);
                             END;
                     END;
                     --
                     -- Remove temp linked server
                     --
                     IF EXISTS (
                               SELECT NULL
                                 FROM [sys].[servers]
                                WHERE [name] = @linkedServerName
                               ) 
                         BEGIN
                             EXEC [sp_dropserver] 
                                  @server = @linkedServerName, 
                                  @droplogins = 'droplogins';
                     END;
                     --
                     -- Next file
                     --
                     SELECT @rowcount = NULL, 
                            @type = 0, 
                            @message = 'Finished file';
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
                     FETCH NEXT FROM csrFiles INTO @rownumber, 
                                                   @filename;
                 END;
             CLOSE csrFiles;
             DEALLOCATE csrFiles;
             IF @numfiles = 0
                 BEGIN
                     SELECT @type = 0, 
                            @logtime = GETDATE(), 
                            @source = NULL, 
                            @destination = NULL, 
                            @message = 'No files found in folder ' + @foldername;
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
             SELECT @type = 0, 
                    @logtime = GETDATE(), 
                    @source = NULL, 
                    @destination = NULL, 
                    @message = 'Finished folder ' + @foldername;
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
             FETCH NEXT FROM csrFolders INTO @rownumber, 
                                             @foldername;
         END;
     CLOSE csrFolders;
     DEALLOCATE csrFolders;
     SELECT @rowcount = NULL, 
            @source = NULL, 
            @destination = NULL, 
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
     SET NOCOUNT OFF;
     RETURN;
GO 
PRINT '\etl.ap_GetMaxColumnLengths.StoredProcedure.sql' 
GO 
 
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
PRINT '\etl.MergeCounts.View.sql' 
GO 
 
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