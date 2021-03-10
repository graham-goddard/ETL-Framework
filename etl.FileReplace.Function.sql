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