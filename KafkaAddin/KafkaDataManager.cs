using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaAddin
{
    //KafkaDataSource class handles the Kafka connection, subscription and consumption. 
    //NewDataReceived event is triggered every time new data is received form the Kafka topic. 
    public class KafkaDataSource : IDisposable
    {
        public delegate void NewDataReceivedEventHandler(object sender, (string key, DateTime time, double value)[] data);

        public event NewDataReceivedEventHandler NewDataReceived;

        private IConsumer<string, string> _consumer;
        private readonly string _topic;
        private readonly string _bootstrapServers;
        public readonly Dictionary<string, (DateTime time, double value)> _latestDataByKeys;
        private CancellationTokenSource _cancellationTokenSource;

        public KafkaDataSource(string bootstrapServers, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
            _latestDataByKeys = new Dictionary<string, (DateTime time, double value)>();
        }

        public void Dispose()
        {
            _consumer?.Dispose();
        }

        public void Start()
        {
            _cancellationTokenSource = new CancellationTokenSource();

            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            _consumer = new ConsumerBuilder<string, string>(config).Build();
            _consumer.Subscribe(_topic);

            Task.Run(() => GetDataAsync(_cancellationTokenSource.Token));
        }

        public void Stop()
        {
            _cancellationTokenSource?.Cancel();
        }

        private async Task GetDataAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = new List<(string key, DateTime time, double value)>();

                var consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult.Message != null)
                {
                    var message = JsonConvert.DeserializeObject<dynamic>(consumeResult.Message.Value);

                    var key = (string)message.key;
                    var time = (DateTime)message.value.time;
                    var value = message.value.value;

                    if (_latestDataByKeys.ContainsKey(key) && _latestDataByKeys[key].time >= time)
                    {
                        continue; //skip older data
                    }

                    _latestDataByKeys[key] = (time, value);
                    result.Add((key, time, value));
                }

                if (result.Any())
                {
                    NewDataReceived?.Invoke(this, result.ToArray());
                }

                await Task.Delay(100, cancellationToken);
            }
        }

    }
}
