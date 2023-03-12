using ExcelDna.Integration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaAddin
{

    // ExcelFunctions class for UDFs
    public static class ExcelFunctions
    {

        // ===============================
        //Step 2 and Step 3
        //Excel UDF that listen to the Kafa topic produced by KafkaPublisher(Step1) 
        //Writes to SQL server table:kafka_data_table 
        //only set to UPSERT the latest data by key then it's used for
        //retrieving kafka data when Excel is just open or Kafka data for the key is not currently being published. 
        // ===============================

        public static object GetKafkaDataByKey(string key)
        {
            //Seems using mandatory unless RTD is registered.
            return XlCall.RTD("KafkaAddin.KafkaExcelRtdServer", null, key);
        }

    }
}
