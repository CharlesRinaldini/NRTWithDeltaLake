--Next load new records
INSERT INTO [dbo].[dbrDemoTransactions]
        ([TransactionName]
        ,[TransactionAmount])
SELECT [TransactionName], [TransactionAmount]
FROM (
    VALUES 
	('Test Tran 3', 123.45),
	('Test Tran 4', 456.78)
) AS t ([TransactionName], [TransactionAmount])
GO

INSERT INTO [dbo].[dbrDemoTransactionsDt]
        ([TransactionName]
        ,[TransactionAmount])
SELECT [TransactionName], [TransactionAmount]
FROM (
    VALUES 
	('Test Tran 3', 123.45),
	('Test Tran 4', 456.78)
) AS t ([TransactionName], [TransactionAmount])
GO

