using log4net.Appender.Extensions;
using log4net.Appender.Language;
using log4net.Core;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace log4net.Appender
{
    public class AzureTableAppender : BufferingAppenderSkeleton
    {
        private CloudStorageAccount _account;
        private CloudTableClient _client;
        private CloudTable _table;
        private DateTime _tableDate;
        private Thread _asyncLoggingThread;
        private List<LoggingEvent> _asyncQueue;
        private AutoResetEvent _asyncItemAvailable;
        private CancellationTokenSource _shutdownTokenSource;
        private HashSet<string> _columns;

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
        public int AsyncIntervalMilliseconds { get; set; }

	    private PartitionKeyTypeEnum _partitionKeyType = PartitionKeyTypeEnum.LoggerName;
        public PartitionKeyTypeEnum PartitionKeyType
        {
            get { return _partitionKeyType; }
            set { _partitionKeyType = value; }
        }

        public string Columns
        {
            get
            {
                return _columns != null ? string.Join(",", _columns) : "";
            }
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    _columns = null;
                }
                else
                {
                    _columns = new HashSet<string>(value.Split(',').Select(v => v.Trim()));
                }
            }
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

        private void SendEvents(IEnumerable<LoggingEvent> events)
        {
            var currentBatch = new List<LoggingEvent>();
            foreach (var evt in events.OrderBy(e => e.TimeStamp))
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

        protected override void SendBuffer(LoggingEvent[] events)
        {
            if (_asyncQueue != null)
            {
                lock (_asyncQueue)
                {
                    _asyncQueue.AddRange(events);
                }
                _asyncItemAvailable.Set();
            }
            else
            {
                SendEvents(events);
            }
        }

        private ITableEntity GetLogEntity(LoggingEvent @event)
        {
            if (Layout != null)
            {
                return new AzureLayoutLoggingEventEntity(@event, PartitionKeyType, Layout);
            }

            return PropAsColumn
                ? (ITableEntity)new AzureDynamicLoggingEventEntity(@event, PartitionKeyType, _columns)
                : new AzureLoggingEventEntity(@event, PartitionKeyType);
        }

        public override void ActivateOptions()
        {
            base.ActivateOptions();

            _account = CloudStorageAccount.Parse(ConnectionString);
            _client = _account.CreateCloudTableClient();
            UpdateTableReference(DateTime.UtcNow);

            if (AsyncIntervalMilliseconds > 0)
            {
                BufferSize = 1;
                _asyncLoggingThread = new Thread(AsyncLogger);
                _asyncQueue = new List<LoggingEvent>();
                _asyncItemAvailable = new AutoResetEvent(false);
                _shutdownTokenSource = new CancellationTokenSource();

                Thread.MemoryBarrier();
                _asyncLoggingThread.Start();
            }
        }

        protected override void OnClose()
        {
            if (_asyncLoggingThread != null)
            {
                _shutdownTokenSource.Cancel();
                _asyncLoggingThread.Join();

                _asyncLoggingThread = null;
                _asyncQueue = null;

                _asyncItemAvailable.Dispose();
                _asyncItemAvailable = null;

                _shutdownTokenSource.Dispose();
                _shutdownTokenSource = null;
            }
        }

        private void AsyncLogger()
        {
            var handles = new [] { _asyncItemAvailable, _shutdownTokenSource.Token.WaitHandle };
            for (;;)
            {
                WaitHandle.WaitAny(handles);
                List<LoggingEvent> events;
                lock (_asyncQueue)
                {
                    events = _asyncQueue.ToList();
                }

                if (events.Count > 0)
                {
                    try
                    {
                        SendEvents(events);

                        lock (_asyncQueue)
                        {
                            _asyncQueue.RemoveRange(0, events.Count);
                        }
                    }
                    catch (Exception)
                    {
                        // Not really anything we can do about this.
                    }
                }

                if (_shutdownTokenSource.Token.IsCancellationRequested)
                {
                    break;
                }

                _shutdownTokenSource.Token.WaitHandle.WaitOne(AsyncIntervalMilliseconds);
            }
        }
    }
}