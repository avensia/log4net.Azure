using log4net.Appender.Extensions;
using log4net.Appender.Language;
using log4net.Core;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace log4net.Appender
{
    public class AzureTableAppender : BufferingAppenderSkeleton
    {
        private CloudStorageAccount _account;
        private CloudTableClient _client;
        private CloudTable _table;
        private DateTime _tableDate;

        public string ConnectionStringName { get; set; }

        private string _connectionString;

        public string ConnectionString
        {
            get
            {
                if (!String.IsNullOrWhiteSpace(ConnectionStringName))
                {
                    return Util.GetConnectionString(ConnectionStringName);
                }
                if (String.IsNullOrEmpty(_connectionString))
                    throw new ApplicationException(Resources.AzureConnectionStringNotSpecified);
                return _connectionString;
            }
            set
            {
                _connectionString = value;
            }
        }


        private string _tableName;

	    public string TableName
        {
            get
            {
                if (String.IsNullOrEmpty(_tableName))
                    throw new ApplicationException(Resources.TableNameNotSpecified);
                return _tableName;
            }
            set
            {
                _tableName = value;
            }
        }

        public bool PropAsColumn { get; set; }
        public bool UseRollingTable { get; set; }

	    private PartitionKeyTypeEnum _partitionKeyType = PartitionKeyTypeEnum.LoggerName;
        public PartitionKeyTypeEnum PartitionKeyType
        {
            get { return _partitionKeyType; }
            set { _partitionKeyType = value; }
        }

        private void UpdateTableReference(DateTime dateTimeUtc)
        {
            if (UseRollingTable)
            {
                var date = dateTimeUtc.Date;
                if (date != _tableDate)
                {
                    _table = _client.GetTableReference(TableName + dateTimeUtc.ToString("yyyyMMdd"));
                    _table.CreateIfNotExists();
                    _tableDate = date;
                }
            }
            else
            {
                if (_table == null)
                {
                    _table = _client.GetTableReference(TableName);
                    _table.CreateIfNotExists();
                }
            }
        }

        private void SendBatch(IEnumerable<LoggingEvent> batch)
        {
            var batchOperation = new TableBatchOperation();
            foreach (var azureLoggingEvent in batch.Select(GetLogEntity))
            {
                batchOperation.Insert(azureLoggingEvent);
            }
            _table.ExecuteBatch(batchOperation);
        }

        protected override void SendBuffer(LoggingEvent[] events)
        {
            var currentBatch = new List<LoggingEvent>();
            foreach (var evt in events)
            {
                var timestampUtc = evt.TimeStamp.ToUniversalTime();
                if (UseRollingTable && timestampUtc.Date != _tableDate)
                {
                    if (currentBatch.Count > 0)
                    {
                        SendBatch(currentBatch);
                        currentBatch.Clear();
                    }
                    UpdateTableReference(timestampUtc);
                }

                currentBatch.Add(evt);
                if (currentBatch.Count == 100)
                {
                    SendBatch(currentBatch);
                    currentBatch.Clear();
                }
            }

            if (currentBatch.Count > 0)
            {
                SendBatch(currentBatch);
            }
        }

        private ITableEntity GetLogEntity(LoggingEvent @event)
        {
            if (Layout != null)
            {
                return new AzureLayoutLoggingEventEntity(@event, PartitionKeyType, Layout);
            }

            return PropAsColumn
                ? (ITableEntity)new AzureDynamicLoggingEventEntity(@event, PartitionKeyType)
                : new AzureLoggingEventEntity(@event, PartitionKeyType);
        }

        public override void ActivateOptions()
        {
            base.ActivateOptions();

            _account = CloudStorageAccount.Parse(ConnectionString);
            _client = _account.CreateCloudTableClient();
            UpdateTableReference(DateTime.UtcNow);
        }
    }
}