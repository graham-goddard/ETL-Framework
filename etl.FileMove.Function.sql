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