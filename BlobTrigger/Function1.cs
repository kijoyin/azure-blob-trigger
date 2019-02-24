using System;
using System.Collections.Generic;
using System.IO;
using CsvHelper;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlobTrigger
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run([BlobTrigger("expenses/{name}.csv", Connection = "AzureWebJobsStorage")]Stream myBlob, string name,
            [Table("Expenses", Connection = "AzureWebJobsStorage")] IAsyncCollector<Expense> expenseTable,
            ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            var records = new List<Expense>();
            using (var memoryStream = new MemoryStream())
            {
                using (var tr = new StreamReader(myBlob))
                {
                    using (var csv = new CsvReader(tr))
                    {
                        if (csv.Read())
                        {
                            log.LogInformation("Reading CSV");
                            csv.ReadHeader();
                            while (csv.Read())
                            {
                                var record = new Expense
                                {
                                    Amount = double.Parse(csv.GetField("Debit")),
                                    Title = csv.GetField("Title"),
                                    PartitionKey = "Expenses",
                                    RowKey = Guid.NewGuid().ToString()
                                };
                                expenseTable.AddAsync(record).Wait();
                                records.Add(record);
                            }
                        }
                    }
                }
            }
        }
    }

    public class Expense: TableEntity
    {
        public string Title { get; set; }
        public double Amount { get; set; }
    }
}
