using log4net.Appender.Extensions;
using log4net.Appender.Language;
using log4net.Core;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using log4net.Util;

namespace log4net.Appender
{
    public class AzureTableAppender : BufferingAppenderSkeleton
    {
        private CloudStorageAccount _account;
        private CloudTableClient _client;
        private CloudTable _table;
        private DateTime _tableDate;
        private Thread _asyncLoggingThread;
        private List<ITableEntity> _asyncQueue;
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
        public string LogLogFile { get; set; }

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

        private void UpdateTableReference(DateTimeOffset dateTimeUtc)
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

        private void SendBatch(IEnumerable<ITableEntity> batch)
        {
            var batchOperation = new TableBatchOperation();
            foreach (var azureLoggingEvent in batch)
            {
                batchOperation.Insert(azureLoggingEvent);
            }
            _table.ExecuteBatch(batchOperation);
        }

        private void SendEvents(IEnumerable<ITableEntity> events)
        {
            var currentBatch = new List<ITableEntity>();
            string partitionKey = null;
            foreach (var evt in events.OrderBy(e => e.Timestamp))
            {
                bool newTable = UseRollingTable && evt.Timestamp.Date != _tableDate;
                bool newPartition = partitionKey != null && evt.PartitionKey != partitionKey;
                if (newTable || newPartition)
                {
                    if (currentBatch.Count > 0)
                    {
                        SendBatch(currentBatch);
                        currentBatch.Clear();
                    }
                    if (newTable)
                    {
                        UpdateTableReference(evt.Timestamp);
                    }
                }
                partitionKey = evt.PartitionKey;

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
                    _asyncQueue.AddRange(events.Select(GetLogEntity));
                }
                _asyncItemAvailable.Set();
            }
            else
            {
                SendEvents(events.Select(GetLogEntity));
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
                _asyncQueue = new List<ITableEntity>();
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
                List<ITableEntity> events;

                if (!string.IsNullOrEmpty(LogLogFile))
                {
                    try
                    {
                        File.SetLastWriteTimeUtc(LogLogFile, DateTime.UtcNow);
                    }
                    catch (Exception)
                    {
                        // Not much to do
                    }
                }

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
                    catch (Exception ex)
                    {
                        if (!string.IsNullOrEmpty(LogLogFile))
                        {
                            try
                            {
                                File.AppendAllLines(LogLogFile, events.Select(e => e.ToString()).Concat(new[] { "Logging Exception: " + ex }));
                                lock (_asyncQueue)
                                {
                                    _asyncQueue.RemoveRange(0, events.Count);
                                }
                            }
                            catch
                            {
                                LogLog.Error(typeof(AzureTableAppender), "Error logging to table storage", ex);
                            }
                        }
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