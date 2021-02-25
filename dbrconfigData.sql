INSERT INTO [dbo].[Entities]
           ([FromEntityName]
           ,[ToEntityName]
           ,[FromZone]
           ,[ToZone]
           ,[WatermarkType]
           ,[TimestampColumn]
           ,[PrimaryKeys])
     SELECT [FromEntityName] = 'dbrDemoTransactions'
           ,[ToEntityName] = 'demo.dbrDemoTransactions'
           ,[FromZone] = 'src'
           ,[ToZone] = 'silver'
           ,[WatermarkType] = 'CT'
           ,[TimestampColumn] = NULL
           ,[PrimaryKeys] = 'TransactionId'
	UNION ALL
     SELECT [FromEntityName] = 'dbrDemoTransactionsDt'
           ,[ToEntityName] = 'demo.dbrDemoTransactionsDt'
           ,[FromZone] = 'src'
           ,[ToZone] = 'silver'
           ,[WatermarkType] = 'TMSTP'
           ,[TimestampColumn] = 'TransactionDatetime'
           ,[PrimaryKeys] = 'TransactionId'
GO
