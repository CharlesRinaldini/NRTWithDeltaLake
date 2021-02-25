# Databricks notebook source
# DBTITLE 1,Library Imports
#Spark SQL imports
from pyspark.sql.types import *
from pyspark.sql.functions import *

#datetime functions for folder and file naming 
from datetime import datetime

#imports for running stored procedures
import pyodbc

#import for interacting with Delta Lake
import delta

# COMMAND ----------

# DBTITLE 1,Variable Loading
#get run information #todo: create actual ephemeral notebook url
notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
try:
  #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
  pipelineOrNotebookName = str(notebook_info["tags"]["jobId"])
except:
  pipelineOrNotebookName = "-1"

#set config db variables and prperties
configSQLHostName = "<yourconfigdatabase>.database.windows.net"
configSQLDatabase = "dbrconfig"
configSQLUsername = "dbrdemo"
configSQLPassword = "<yourpassword>"
config_jdbcUrl = "jdbc:sqlserver://{0};database={1};user={2};password={3}".format(configSQLHostName, configSQLDatabase, configSQLUsername, configSQLPassword)
config_connectionProperties = {
  "user" : configSQLUsername,
  "password" : configSQLPassword,    
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

#work around for getting this newest driver from the installed pyodbc from above
pyodbc.drivers()
[item for item in pyodbc.drivers()][-1]
driver = [item for item in pyodbc.drivers()][-1]

#connection for running stored procedures via PyODBC
configConn = pyodbc.connect( 'DRIVER={'+driver+'};'
                       'SERVER='+configSQLHostName+';'
                       'DATABASE='+configSQLDatabase+';UID='+configSQLUsername+';'
                       'PWD='+configSQLPassword+'')

#set variables 
storageAccountName = "<yourstorageaccountname>"
storageSPNAppId = "<yourspnappid>"
storageTenantId = "<yourtenantid>"
storageSPNKey = "<yourspnkey>"

#create mounts
tempMnt = "/mnt/temp"
silverMnt = "/mnt/silverMnt"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": storageSPNAppId,
           "fs.azure.account.oauth2.client.secret": storageSPNKey,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+storageTenantId+"/oauth2/token"}

#mount to storage
if not any(mount.mountPoint == silverMnt for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = "abfss://silver@"+storageAccountName+".dfs.core.windows.net/",
    mount_point = silverMnt,
    extra_configs = configs)
if not any(mount.mountPoint == tempMnt for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = "abfss://temp@"+storageAccountName+".dfs.core.windows.net/",
    mount_point = tempMnt,
    extra_configs = configs)

#set date and time values for paths and file names
now = datetime.now()
folderHNS = now.strftime("yyyy=%Y/MM=%m/dd=%d/")
fileDatetime = now.strftime("_%Y%m%d%H%M%S.parquet")

# COMMAND ----------

# DBTITLE 1,Declare Functions
def executeSQL(i_sqlSelect, i_jdbcUrl, i_jdbcConnectionProperties):
  i_sqlSelect = "(" + i_sqlSelect + ") AS SQLTable"
  o_df = spark.read.jdbc(url=i_jdbcUrl, table=i_sqlSelect, properties=i_jdbcConnectionProperties)
  return o_df

# COMMAND ----------

# DBTITLE 1,Get Source Data from SQL Server to load to Data Lake Prep Zone
def loadToSilverZone(row):
  newWatermark = "0"
  oldWatermark = "0"
    
  #set the server/database for each iteration
  entityId = row.EntityId
  fromEntityName = row.FromEntityName
  toEntityName = row.ToEntityName
  fromZone = row.FromZone
  toZone = row.ToZone
  watermarkType = row.WatermarkType
  timestampColumn = row.TimestampColumn
  primaryKeys = row.PrimaryKeys
  oldWatermark = row.Watermark

  entitySQLHostName = "<yourdatabase>.database.windows.net"
  entitySQLDatabase = "dbrdemo"
  entitySQLUsername = "dbrdemo"
  entitySQLPassword = "<yourpassword>"
  
  #newFileName = entitySQLDatabase + "." + sourceSchemaZone + "." + entityName
  databaseName = (toEntityName.split("."))[0]
  tableName = (toEntityName.split("."))[1]
  silverPath = f"{silverMnt}/{databaseName}.{tableName}"
    
  #construct the jdbc connection information
  entity_jdbcUrl = "jdbc:sqlserver://{0};database={1};user={2};password={3}".format(entitySQLHostName, entitySQLDatabase, entitySQLUsername, entitySQLPassword)
  entity_connectionProperties = {
    "user" : entitySQLUsername,
    "password" : entitySQLPassword,    
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  }
  #try:        
  #get new watermark from the source entity
  if watermarkType == "CT":
    wmSQL = "SELECT Watermark = CHANGE_TRACKING_CURRENT_VERSION()"
  elif watermarkType == "TMSTP":
    wmSQL = f"SELECT Watermark = MAX(CONVERT(VARCHAR(21), {timestampColumn}, 120)) FROM {fromEntityName} WHERE {timestampColumn} > '{oldWatermark}'"
  watermarkDF = executeSQL(wmSQL, entity_jdbcUrl, entity_connectionProperties).collect()

  newWatermark = str(watermarkDF[0]["Watermark"])

  #connection for running stored procedures via PyODBC
  configConn = pyodbc.connect( 'DRIVER={'+driver+'};'
                         'SERVER='+configSQLHostName+';'
                         'DATABASE='+configSQLDatabase+';UID='+configSQLUsername+';'
                         'PWD='+configSQLPassword+'')

  #insert row into Watermark table
  openCursor = configConn.cursor()
  openSql = """EXEC dbo.OpenWatermark
     @EntityId = N'""" + str(entityId) + """'
    ,@Watermark = N'""" + str(newWatermark)[:19] + """'   
    """  
  configConn.autocommit = True
  openCursor.execute(openSql)
  returnvalue = openCursor.fetchall()
  WatermarkID = int(returnvalue[0][0])
  openCursor.close()

  print(f"Created WatermarkId: {str(WatermarkID)} OldWM: {oldWatermark}, NewWM: {newWatermark}")

  #Only process changes if there is a new watermark
  if oldWatermark != newWatermark:            
    #if there hasn't been a watermark set do a complete load of the entity
    if oldWatermark == "0" or oldWatermark == "2000-01-01":
      entitySQL = "SELECT *, GETUTCDATE() as SyncDateTime, 'I' as SyncOperation FROM " + fromEntityName
    #if the table is CDC then construct a ChangeTracking query 
    elif watermarkType == "CT":
      pKs = primaryKeys.split(",")
      join = ""
      #loop the list of the connections 
      for pK in pKs:
        join = join + "ct." + pK + " = x." + pK + " AND "

      join = join[:-5]

      entitySQL = """SELECT x.*, GETUTCDATE() as SyncDateTime, ct.SYS_CHANGE_OPERATION as SyncOperation 
      FROM CHANGETABLE(CHANGES """ + fromEntityName + """, """ + oldWatermark + """) as ct 
      LEFT JOIN """ + fromEntityName + """ x (NOLOCK) 
      ON """ + join 
    elif watermarkType == "TMSTP":
      entitySQL = "SELECT *, GETUTCDATE() as SyncDateTime, 'I' as SyncOperation FROM " + fromEntityName + " WHERE CONVERT(VARCHAR(21), " + timestampColumn + ", 120) > '" + oldWatermark + "'"

    print(f"Using SQL: {entitySQL}")
    #execute whichever query was selected and count the number of rows in the dataframe
    newRecordsDF = executeSQL(entitySQL, entity_jdbcUrl, entity_connectionProperties)      
    newRecordsDF.cache()
    
    #try:
    #get the first part of the target entity name
    databaseName = (toEntityName.split("."))[0]
    #if you create a delta lake folder with a prefix it's considered the database name 
    dbSql = f"CREATE DATABASE IF NOT EXISTS {databaseName}" 
    #create the database if it doesn't exist - this will keep things organized in the future 
    spark.sql(dbSql)
    if (toEntityName.lower().split("."))[1] not in [t.name for t in spark.catalog.listTables((toEntityName.split("."))[0])]:
      print("Creating or overwriting delta table")
      #if there hasn't been a processing of a silver entity then we have to create the delta table from scratch
      newRecordsDF.write.format("delta").mode('overwrite').save(silverPath)
      #create the delta table from the delta folder if it hasn't been created already
      deltaSql = f"CREATE TABLE IF NOT EXISTS {toEntityName} USING DELTA LOCATION '{silverPath}'"        
      spark.sql(deltaSql)
    else:          
      print("Updating Delta Table")
      #merge the new records into the delta table
      deltaTable = delta.DeltaTable.forPath(spark, silverPath)

      joinConditions = ""
      for col in primaryKeys.split(","):
        joinConditions = joinConditions + "source." + col + " = target." + col + " AND "
      joinConditions = joinConditions[:-5]       

      deltaTable.alias("target").merge(
        newRecordsDF.alias("source"),joinConditions
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

  #close the watermark
  closeCursor = configConn.cursor()
  closeSql = """EXEC dbo.CloseWatermark
    @WatermarkID = ?
    """  
  configConn.autocommit = True
  closeCursor.execute(closeSql, WatermarkID)
  closeCursor.close()


# COMMAND ----------

entitiesSQL = """SELECT e.EntityId
,e.FromEntityName
,e.ToEntityName
,e.FromZone
,e.ToZone
,e.WatermarkType
,e.TimestampColumn
,e.PrimaryKeys
,Watermark = COALESCE(wo.Watermark, CASE e.WatermarkType WHEN 'CT' THEN '0' ELSE '2000-01-01' END)
FROM [dbo].[Entities] e
LEFT JOIN (
	SELECT wi.EntityId
		,Watermark = CASE 
			WHEN WatermarkType = 'TMSTP' 
				THEN CONVERT(VARCHAR(21), DateWatermark, 120)
			WHEN WatermarkType = 'CT'
				THEN CAST(CTWatermark AS VARCHAR(21))
			END 
		,RN = ROW_NUMBER() OVER(PARTITION BY wi.EntityId ORDER BY LoadStartDatetime DESC)
	FROM [dbo].[Watermarks] wi
	JOIN [dbo].[Entities] e
	ON e.EntityId = wi.EntityId
	WHERE LoadEndDatetime IS NOT NULL
) wo
ON e.EntityId = wo.EntityId
AND wo.RN = 1
"""
entitiesDF = executeSQL(entitiesSQL, config_jdbcUrl, config_connectionProperties)
entitiesDF.display()

# COMMAND ----------

# DBTITLE 1,Get Source Data from SQL Server to load to Data Lake Prep Zone
#convert entities into a list to process
entitiesList = entitiesDF.collect()

for entity in entitiesList:
  loadToSilverZone(entity)

# COMMAND ----------

dbutils.notebook.exit("Success")
