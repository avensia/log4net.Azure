using System;
using System.Collections;
using System.Collections.Generic;
using log4net.Core;

namespace log4net.Appender
{
    internal sealed class AzureDynamicLoggingEventEntity : ElasticTableEntity
    {
        public AzureDynamicLoggingEventEntity(LoggingEvent e, PartitionKeyTypeEnum partitionKeyType, HashSet<string> columns)
        {
            void Add(string key, object value)
            {
                if (columns == null || columns.Contains(key))
                {
                    this[key] = value;
                }
            }

            Add("Domain", e.Domain);
            Add("Identity", e.Identity);
            Add("Level", e.Level.ToString());
            Add("LoggerName", e.LoggerName);
            Add("Message", e.RenderedMessage);
            Add("EventTimeStamp", e.TimeStamp.ToUniversalTime());
            Add("ThreadName", e.ThreadName);
            Add("UserName", e.UserName);
            Add("Location", e.LocationInformation.FullInfo);

            if (e.ExceptionObject != null)
            {
                Add("Exception", e.GetExceptionString());
            }
            
            foreach (DictionaryEntry entry in e.Properties)
            {
                var key = entry.Key.ToString()
                    .Replace(":", "_")
                    .Replace("@", "_")
                    .Replace(".", "_");

                var value = entry.Value;

                if (value != null && !(value is byte[]) && !(value is bool) && !(value is DateTimeOffset)  && !(value is DateTime)  && !(value is double) && !(value is Guid)  && !(value is int)  && !(value is long)  && !(value is string))
                {
                    value = e.Repository?.RendererMap.FindAndRender(value) ?? value.ToString();
                }

                Add(key, value);
            }

            Timestamp = e.TimeStamp.ToUniversalTime();
            PartitionKey = e.MakePartitionKey(partitionKeyType);
            RowKey = e.MakeRowKey();
        }
    }
}