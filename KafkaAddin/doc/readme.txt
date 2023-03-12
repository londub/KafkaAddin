1. KafkaExcelRtdServer class to setup ExcelRTD server that also handles the database operation to keep latest data for each keys in 
the SQL database that's used to show inital values when Excel is just open or the Kafa data is not being published. 
  
2. KafkaDataSource class handles the Kafka connection, subscription and consumption. 
NewDataReceived event is triggered every time new data is received form the Kafka topic. 

3. DatabaseManager class that handles the SQL server connection and executes queries to keep the latest data for each records.

4. ExcelFunctions class for UDFs

5. table: CREATE TABLE kafka_data_table (
    [key] varchar(255),
    time datetime,
    value float
);

6. Kafka: <add key="BootstrapServers" value="localhost:9092" />
          <add key="KafkaTopic" value="test-dev-topic" />

7. NuGet Packages:
Confluent.Kafka v2.0.2
ExcelDna.AddIn  v1.6.0
Newtonsoft.Json v13.0.3

===============================
Step 2 and Step 3:
Excel UDF that listen to the Kafa topic produced by KafkaPublisher(Step1) 
Writes to SQL server table:kafka_data_table that is only set to UPSERT with latest data by key then it's used for
retrieving kafka data when Excel is just open or Kafka data for the key is not currently being published. 
