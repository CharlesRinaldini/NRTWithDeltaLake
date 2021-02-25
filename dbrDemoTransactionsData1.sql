--First set of initial transactions
INSERT INTO [dbo].[dbrDemoTransactions]
        ([TransactionName]
        ,[TransactionAmount])
SELECT [TransactionName], [TransactionAmount]
FROM (
    VALUES 
	('Test Tran 1', 420.69),
	('Test Tran 2', 694.20)
) AS t ([TransactionName], [TransactionAmount])
GO

INSERT INTO [dbo].[dbrDemoTransactionsDt]
        ([TransactionName]
        ,[TransactionAmount])
SELECT [TransactionName], [TransactionAmount]
FROM (
    VALUES 
	('Test Tran 1', 420.69),
	('Test Tran 2', 694.20)
) AS t ([TransactionName], [TransactionAmount])
GO