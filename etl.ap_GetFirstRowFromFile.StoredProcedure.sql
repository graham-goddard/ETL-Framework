IF OBJECT_ID('[etl].[ap_GetFirstRowFromFile]', 'P') IS NOT NULL
    DROP PROCEDURE [etl].[ap_GetFirstRowFromFile];
GO

CREATE PROCEDURE [etl].[ap_GetFirstRowFromFile] 
                 @path            VARCHAR(260), 
                 @filename        VARCHAR(260), 
                 @codepage        VARCHAR(10)  = '65001', 
                 @rowTerminator   VARCHAR(6)   = '0x0d0a', 
                 @fieldterminator CHAR(1)      = ',', 
                 @firstRow        VARCHAR(MAX) OUTPUT, 
                 @debug           BIT          = 0
AS
    BEGIN
        DECLARE @sqlCmd VARCHAR(MAX), @xml AS XML, @dedupedcols VARCHAR(MAX)= '';
        CREATE TABLE [#IMPORT](
                     [FIRSTROW] NVARCHAR(MAX)
        );
        SET @sqlCmd = 'BULK INSERT #IMPORT FROM ''' + @path + '\' + @filename + ''' WITH(CODEPAGE = ''' + @codepage + ''', DATAFILETYPE = ''char'', FIRSTROW = 1, LASTROW = 1, FIELDTERMINATOR = ''\0'', ROWTERMINATOR = ''' + @rowTerminator + ''');';
        IF @debug = 1
            BEGIN
                PRINT 'CREATE TABLE [#IMPORT]([FIRSTROW] NVARCHAR(MAX));';
                PRINT @sqlCmd;
        END;
        EXEC (@sqlCmd);
        SELECT @firstRow = [FIRSTROW]
          FROM [#IMPORT];
        DROP TABLE [#IMPORT];
        SELECT @firstRow = REPLACE(@firstRow, '&', '&amp;');
        IF @debug = 1
            BEGIN
                PRINT 'DROP TABLE [#IMPORT];';
        END;
        SET @xml = CAST(('<X>' + replace(@firstrow, @fieldterminator, '</X><X>') + '</X>') AS XML);
        WITH colnames
             AS (SELECT [N].value
                        ('.', 'varchar(128)') AS value, 
                        [colseq] = ROW_NUMBER() OVER(
                        ORDER BY (
                                 SELECT 1
                                 ) )
                   FROM @xml.nodes
                        ('X') AS [T]([N])),
             colnameswithdup
             AS (SELECT *, 
                        [dup] = ROW_NUMBER() OVER(PARTITION BY value
                        ORDER BY (
                                 SELECT 1
                                 ) )
                   FROM [colnames]),
             uniquecolnames
             AS (SELECT *, 
                        [uniquecolname] = [value] + CASE
                                                        WHEN [dup] = 1 THEN ''
                                                        ELSE '_' + CAST([dup] - 1 AS VARCHAR(3))
                                                    END
                   FROM [colnameswithdup])
             SELECT @dedupedcols = @dedupedcols + @fieldterminator + [uniquecolname]
               FROM [UniqueColNames]
             ORDER BY [colseq];
        SELECT @firstRow = STUFF(@dedupedcols, 1, LEN(@fieldterminator), '');
        RETURN;
    END;
GO