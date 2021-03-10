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