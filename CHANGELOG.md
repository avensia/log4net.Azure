# Change log

## 3.0.0

- Upgrade WindowsAzure.Storage version to match newer Episerver versions


## 2.0.4

- Add try-catch around CloudTable.CreateIfNotExists()
  See:
  https://stackoverflow.com/questions/48893519/azure-table-storage-exception-409-conflict-unexpected


## 2.0.3

- Add threshold of 1000 log entries per batch to fix performance bottle neck

## 2.0.2

- Downgrade Microsoft.WindowsAzure.Storage to accomply with the present latest Episerver.Azure package

## 2.0.1

- Upgrade WindowsAzure.Storage to 9.0.0

## 1.0.1

- Use unescaped log4net property names to filter columns in AzureTableAppender

## 1.0.0

Initial version