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