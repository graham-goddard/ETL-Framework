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
