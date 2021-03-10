IF OBJECT_ID('[etl].[MergeCounts]', 'V') IS NOT NULL
    DROP VIEW [etl].[MergeCounts];
GO

IF OBJECT_ID('[etl].[logXML]', 'U') IS NOT NULL
    DROP TABLE [etl].[logXML];
GO

IF OBJECT_ID('[etl].[log]', 'U') IS NOT NULL
    DROP TABLE [etl].[log];
GO

IF OBJECT_ID('[etl].[ap_WriteStringToFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_WriteStringToFile];
GO

IF OBJECT_ID('[etl].[ap_InsertETLlog]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_InsertETLlog];
GO

IF OBJECT_ID('[etl].[ap_GenericPopulate]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GenericPopulate];
GO

IF OBJECT_ID('[etl].[ap_GetFirstRowFromFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetFirstRowFromFile];
GO

IF OBJECT_ID('[etl].[ap_ImportFiles]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_ImportFiles];
GO

IF OBJECT_ID('[etl].[ap_ImportExcel]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_ImportExcel];
GO

IF OBJECT_ID('[etl].[ap_GetMaxColumnLengths]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetMaxColumnLengths];
GO

IF OBJECT_ID('[etl].[FileCopy]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileCopy];
GO

IF OBJECT_ID('[etl].[FileDelete]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileDelete];
GO

IF OBJECT_ID('[etl].[FileDeleteMatch]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileDeleteMatch];
GO

IF OBJECT_ID('[etl].[FileMove]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileMove];
GO

IF OBJECT_ID('[etl].[FileReplace]', 'FS') IS NOT NULL
    DROP FUNCTION [etl].[FileReplace];
GO

IF OBJECT_ID('[etl].[fn_GetCastString]', 'FN') IS NOT NULL
    DROP FUNCTION [etl].[fn_GetCastString];
GO
