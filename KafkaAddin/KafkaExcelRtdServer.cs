using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using ExcelDna.Integration;
using ExcelDna.Integration.Rtd;
using Newtonsoft.Json;

namespace KafkaAddin
{

    //KafkaExcelRtdServer class to setup ExcelRTD server handles the database operation to keep latest data for each keys in the SQL database 
    //that's used to show inital values when Excel is just open or the Kafa data is not being published. 
    public class KafkaExcelRtdServer : ExcelRtdServer
    {
        private readonly Dictionary<string, Topic> _topics;
        private readonly KafkaDataSource _dataSource;
        private readonly DatabaseManager _databaseManager;

        private const string _connectionString = "Server=DESKTOP-4QMU3T7\\SQLEXPRESS;Database=my_dev_database;Integrated Security=True;";
        private const string _tablename = "kafka_data_table";

        private const string _kafkaServer = "localhost:9092";
        private const string _kafkaTopic = "test-dev-topic";



        public KafkaExcelRtdServer()
        {
            _topics = new Dictionary<string, Topic>();
            _dataSource = new KafkaDataSource(_kafkaServer, _kafkaTopic);
            _dataSource.NewDataReceived += DataSource_NewDataReceived;
            _dataSource.Start();

            _databaseManager = new DatabaseManager(_connectionString, _tablename);

            var allData = _databaseManager.GetAllData();

            foreach (var data in allData)
            {
                var key = data.key;
                var time = data.time;
                var value = data.value;
                _dataSource._latestDataByKeys[key] = (time, value);
            }
        }

        private void DataSource_NewDataReceived(object sender, (string key, DateTime time, double value)[] data)
        {

            foreach (var datum in data)
            {
                if (_topics.ContainsKey(datum.key))
                {
                    _topics[datum.key].UpdateValue(datum.value);
                }

                _dataSource._latestDataByKeys[datum.key] = (datum.time, datum.value);


                _databaseManager.UpsertData(datum.key, datum.time, datum.value);
            }
        }

        protected override bool ServerStart()
        {
            //not needed for now.
            return true;
        }

        protected override void ServerTerminate()
        {

            //release resources
            _databaseManager?.Dispose();
            _dataSource?.Dispose();
        }

        protected override object ConnectData(Topic topic, IList<string> topicInfo, ref bool newValues)
        {
            string key = topicInfo[0];
            if (!_topics.ContainsKey(key))
            {
                _topics[key] = topic;

            }
            else
            {
                newValues = true;
            }

            if (_dataSource._latestDataByKeys.ContainsKey(key))
            {
                return _dataSource._latestDataByKeys[key].value;
            }

            return null;
        }

        protected override void DisconnectData(Topic topic)
        {
            if (_topics.ContainsValue(topic))
            {
                _topics.Remove(_topics.FirstOrDefault(x => x.Value == topic).Key);
            }
        }
    }
}

