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