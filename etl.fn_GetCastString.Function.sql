IF OBJECT_ID('[etl].[fn_GetCastString]', 'FN') IS NOT NULL
    DROP FUNCTION [etl].[fn_GetCastString];
GO

CREATE FUNCTION [etl].[fn_GetCastString](
                @sourcedatatype    SYSNAME, 
                @destdatatype      SYSNAME, 
                @sourcecolname     SYSNAME, 
                @destcolname       SYSNAME, 
                @destprecision     INT, 
                @destscale         INT, 
                @destcharmaxlength INT, 
                @convertnull       BIT, 
                @rtrim             BIT
)
RETURNS VARCHAR(MAX)
AS
     BEGIN
         DECLARE @output VARCHAR(8000);
         DECLARE @sourcecolname2 VARCHAR(8000);
         SELECT @sourcecolname2 = '[' + @sourcecolname + ']';
         IF @convertnull = 1
            AND @sourcedatatype = 'nvarchar'
             BEGIN
                 SELECT @sourcecolname2 = 'NULLIF([' + @sourcecolname + '],''NULL'')';
         END;
         SELECT @sourcecolname = @sourcecolname2;
         SELECT @output = CASE
                              WHEN @sourcedatatype IS NULL THEN 'NULL /*UNMAPPED COLUMN ' + QUOTENAME(@destcolname) + '*/ '
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype IN('nvarchar', 'nchar', 'varchar', 'char') THEN CASE
                                                                                                         WHEN @rtrim = 1 THEN 'RTRIM('
                                                                                                         ELSE ''
                                                                                                     END + 'TRY_CAST (' + @sourcecolname + ' AS ' + UPPER(@destdatatype) + '(' + COALESCE(LTRIM(STR(NULLIF(@destcharmaxlength, -1))), 'max') + '))' + CASE
                                                                                                                                                                                                                                                          WHEN @rtrim = 1 THEN ')'
                                                                                                                                                                                                                                                          ELSE ''
                                                                                                                                                                                                                                                      END
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'int' THEN 'TRY_CAST (' + @sourcecolname + ' AS int)'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'money' THEN 'TRY_CAST (' + @sourcecolname + ' AS money)'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'numeric' THEN 'TRY_CAST (' + @sourcecolname + ' AS numeric(' + LTRIM(STR(@destprecision)) + ',' + LTRIM(STR(@destscale)) + '))'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'datetime' THEN 'TRY_PARSE (' + @sourcecolname + ' AS datetime USING ''EN-GB'')'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'date' THEN 'TRY_PARSE (' + @sourcecolname + ' AS date USING ''EN-GB'')'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'time' THEN 'TRY_PARSE (' + @sourcecolname + ' AS time USING ''EN-GB'')'
                              WHEN @sourcedatatype = 'nvarchar'
                                   AND @destdatatype = 'bit' THEN 'TRY_CAST (CASE WHEN ' + @sourcecolname + ' IN (''Y'',''Yes'',''True'',''T'',''1'') THEN 1 WHEN ' + @sourcecolname + ' IN (''N'',''No'',''False'',''F'',''0'') THEN 0 ELSE NULL END AS bit)'
                              ELSE '' + @sourcecolname + ''
                          END;
         RETURN @output;
     END;
GO