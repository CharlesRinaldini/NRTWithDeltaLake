CREATE USER dbrdemo
	FOR LOGIN dbrdemo
	WITH DEFAULT_SCHEMA = dbo
GO

-- Add user to the database owner role
EXEC sp_addrolemember N'db_owner', N'dbrdemo'
GO

ALTER DATABASE dbrdemo  
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)  

/*
DROP TABLE dbo.dbrDemoTransactions
DROP TABLE dbo.dbrDemoTransactionsDt
*/

CREATE TABLE dbo.dbrDemoTransactions (
	TransactionId BIGINT IDENTITY(1, 1) NOT NULL
	,TransactionName VARCHAR(100)
	,TransactionAmount FLOAT
	,TransactionDatetime DATETIME2 DEFAULT GETUTCDATE()
    ,CONSTRAINT PK_Transactions PRIMARY KEY CLUSTERED (TransactionId ASC)
)

ALTER TABLE dbo.dbrDemoTransactions 
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON)  

CREATE TABLE dbo.dbrDemoTransactionsDt (
	TransactionId BIGINT IDENTITY(1, 1) NOT NULL
	,TransactionName VARCHAR(100)
	,TransactionAmount FLOAT
	,TransactionDatetime DATETIME2 DEFAULT GETUTCDATE()
    ,CONSTRAINT PK_TransactionsDt PRIMARY KEY CLUSTERED (TransactionId ASC)
)
