CREATE LOGIN dbrdemo
	WITH PASSWORD = 'ThisIsThe1stDemo!' 
GO

CREATE USER dbrdemo
	FOR LOGIN dbrdemo
	WITH DEFAULT_SCHEMA = dbo
GO

-- Add user to the database owner role
EXEC sp_addrolemember N'db_owner', N'dbrdemo'
GO


/*
DROP TABLE dbo.Watermarks
DROP TABLE dbo.Entities
*/

CREATE TABLE dbo.Entities (
	 EntityId BIGINT IDENTITY(1, 1) NOT NULL
	,FromEntityName VARCHAR(100)
	,ToEntityName VARCHAR(100)
	,FromZone VARCHAR(10)
	,ToZone VARCHAR(10)
	,WatermarkType VARCHAR(10)
	,TimestampColumn VARCHAR(100)
	,PrimaryKeys VARCHAR(500)
    ,CONSTRAINT PK_Entities PRIMARY KEY CLUSTERED (EntityId ASC)
)
GO

CREATE TABLE dbo.Watermarks (
	 WatermarkId BIGINT IDENTITY(1, 1) NOT NULL
	,EntityId BIGINT
	,DateWatermark DATETIME2
	,CTWatermark BIGINT
	,LoadStartDatetime DATETIME2
	,LoadEndDatetime DATETIME2
    ,CONSTRAINT PK_Watermarks PRIMARY KEY CLUSTERED (WatermarkId ASC)
    ,CONSTRAINT FK_Watermarks_Entities FOREIGN KEY (EntityId) REFERENCES Entities(EntityId)
)
GO

CREATE PROCEDURE dbo.OpenWatermark
	 @EntityId BIGINT
	,@Watermark VARCHAR(20)
AS 
BEGIN 
	SET NOCOUNT ON; --Needed for pyodbc
	DECLARE @wmType VARCHAR(10)
	SET @wmType = (SELECT WatermarkType FROM dbo.Entities WHERE EntityId = @EntityId)

	IF @wmType = 'CT'
	BEGIN
		INSERT INTO dbo.Watermarks (
			 EntityId
			,CTWatermark
			,LoadStartDatetime
		)
		VALUES (
			 @EntityId
			,@Watermark
			,GETUTCDATE()
		)
		SELECT SCOPE_IDENTITY() AS WatermarkId
	END
	ELSE IF @wmType = 'TMSTP'
	BEGIN
		INSERT INTO dbo.Watermarks (
			 EntityId
			,DateWatermark
			,LoadStartDatetime
		)
		VALUES (
			 @EntityId
			,@Watermark
			,GETUTCDATE()
		)
		SELECT SCOPE_IDENTITY() AS WatermarkId
	END
END
GO

CREATE PROCEDURE dbo.CloseWatermark
	 @WatermarkId BIGINT
AS 
BEGIN 
	UPDATE dbo.Watermarks SET LoadEndDatetime = GETUTCDATE() WHERE WatermarkId = @WatermarkId
END
GO

