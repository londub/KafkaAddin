using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaAddin
{
    //DatabaseManager class that handles the SQL server connection and executes queries to keep the latest data for each records.
    public class DatabaseManager : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly string _tableName;

        public DatabaseManager(string connectionString, string tableName)
        {
            _connection = new SqlConnection(connectionString);
            _tableName = tableName;
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }

        public IEnumerable<(string key, DateTime time, double value)> GetAllData()
        {
            if (_connection.State == System.Data.ConnectionState.Closed)
            {
                _connection.Open();
            }

            using (var command = new SqlCommand($"SELECT [key], [time], [value] FROM {_tableName}", _connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var key = reader.GetString(0);
                    var time = reader.GetDateTime(1);
                    var value = reader.GetDouble(2);
                    yield return (key, time, value);
                }
            }
        }

        public void UpsertData(string key, DateTime time, double value)
        {
            if (_connection.State == System.Data.ConnectionState.Closed)
            {
                _connection.Open();
            }

            using (var command = new SqlCommand($"IF EXISTS (SELECT 1 FROM {_tableName} WHERE [key] = @key) " +
                                                $"UPDATE {_tableName} SET [time] = @time, [value] = @value WHERE [key] = @key " +
                                                $"ELSE " +
                                                $"INSERT INTO {_tableName} ([key], [time], [value]) VALUES (@key, @time, @value)", _connection))
            {
                command.Parameters.AddWithValue("@key", key);
                command.Parameters.AddWithValue("@time", time);
                command.Parameters.AddWithValue("@value", value);
                command.ExecuteNonQuery();
            }
        }
    }

}
