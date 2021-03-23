# Parse raw logs to tabular format
This package helps to parse new line delimited logs to tabular formats. The user provides the regex, file path and column names, and a dataframe will be returned.  
Depending on the supplied mode (local/spark), a pandas dataframe or a spark dataframe will be returned.

## Installation
pip install data-parser

## Usage - Local (Pandas)
```python
from data_parser import DataSource

# Bind 9: Feb  5 09:12:11 ns1 named[80090]: client 192.168.10.12#3261: query: www.server.example IN A
dns = DataSource(
    path='/path/to/dnsdir/*.txt',  # Glob patterns supported
    mode='local'
)

# Pandas dataframe is returned
dns_df = dns.parse(
    regex='^([A-Z][a-z]{2})\s+(\d+) (\d{2}\:\d{2}\:\d{2}) (\S+).+client ([^\s#]+)#(\d+)',
    col_names=['month', 'day', 'time', 'nameserver', 'query_ip', 'port'],
    on_error='raise'
)
```

## Usage - Spark (Pyspark)
```python
from data_parser import DataSource

# Bind 9: Feb  5 09:12:11 ns1 named[80090]: client 192.168.10.12#3261: query: www.server.example IN A
dns = DataSource(
    path='/path/to/dns/log',
    mode='spark'
)

# Spark dataframe is returned
dns_df = dns.parse(
    regex='^([A-Z][a-z]{2})\s+(\d+) (\d{2}\:\d{2}\:\d{2}) (\S+).+client ([^\s#]+)#(\d+)',
    col_names=['month', 'day', 'time', 'nameserver', 'query_ip', 'port'],
    on_error='raise'
)
```
