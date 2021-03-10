
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