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