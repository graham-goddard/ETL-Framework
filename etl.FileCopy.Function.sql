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