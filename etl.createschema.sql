IF NOT EXISTS
(
    SELECT *
    FROM [sys].[schemas]
    WHERE [name] = 'etl'
)
	EXECUTE('CREATE SCHEMA [etl]');
GO