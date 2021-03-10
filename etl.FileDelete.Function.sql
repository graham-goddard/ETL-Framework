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