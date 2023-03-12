1. Kafka: <add key="BootstrapServers" value="localhost:9092" />
          <add key="KafkaTopic" value="test-dev-topic" />

2. NuGet Packages:
Confluent.Kafka v2.0.2
Newtonsoft.Json v13.0.3

// ===============================
//Step1:
//write a c# program that generates like above data and publish to kafka topic time is increasing order date time, value is float number between 0 and 1
//use random generator generating 2 message in a second
// ===============================