<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Getting Started With Hive Beeline

## Getting Beeline


```shell

$ bin/beeline --help
Usage: java org.apache.hive.cli.beeline.BeeLine
   -u <database url>               the JDBC URL to connect to
   -r                              reconnect to last saved connect url (in conjunction with !save)
   -n <username>                   the username to connect as
   -p <password>                   the password to connect as
   -d <driver class>               the driver class to use
   -i <init file>                  script file for initialization
   -e <query>                      query that should be executed
   -f <exec file>                  script file that should be executed
   -w (or) --password-file <password file>  the password file to read password from
   --hiveconf property=value       Use value for given property
   --hivevar name=value            hive variable name and value
                                   This is Hive specific settings in which variables
                                   can be set at session level and referenced in Hive
                                   commands or queries.
   --property-file=<property-file> the file to read connection properties (url, driver, user, password) from
   --color=[true/false]            control whether color is used for display
   --showHeader=[true/false]       show column names in query results
   --headerInterval=ROWS;          the interval between which heades are displayed
   --fastConnect=[true/false]      skip building table/column list for tab-completion
   --autoCommit=[true/false]       enable/disable automatic transaction commit
   --verbose=[true/false]          show verbose error messages and debug info
   --showWarnings=[true/false]     display connection warnings
   --showDbInPrompt=[true/false]   display the current database name in the prompt
   --showNestedErrs=[true/false]   display nested errors
   --numberFormat=[pattern]        format numbers using DecimalFormat pattern
   --force=[true/false]            continue running script even after errors
   --maxWidth=MAXWIDTH             the maximum width of the terminal
   --maxColumnWidth=MAXCOLWIDTH    the maximum width to use when displaying columns
   --silent=[true/false]           be more silent
   --autosave=[true/false]         automatically save preferences
   --outputformat=[table/vertical/csv2/tsv2/dsv/csv/tsv]  format mode for result display
                                   Note that csv, and tsv are deprecated - use csv2, tsv2 instead
   --incremental=[true/false]      Defaults to false. When set to false, the entire result set
                                   is fetched and buffered before being displayed, yielding optimal
                                   display column sizing. When set to true, result rows are displayed
                                   immediately as they are fetched, yielding lower latency and
                                   memory usage at the price of extra display column padding.
                                   Setting --incremental=true is recommended if you encounter an OutOfMemory
                                   on the client side (due to the fetched result set size being large).
                                   Only applicable if --outputformat=table.
   --incrementalBufferRows=NUMROWS the number of rows to buffer when printing rows on stdout,
                                   defaults to 1000; only applicable if --incremental=true
                                   and --outputformat=table
   --truncateTable=[true/false]    truncate table column when it exceeds length
   --delimiterForDSV=DELIMITER     specify the delimiter for delimiter-separated values output format (default: |)
   --isolation=LEVEL               set the transaction isolation level
   --nullemptystring=[true/false]  set to true to get historic behavior of printing null as empty string
   --maxHistoryRows=MAXHISTORYROWS The maximum number of rows to store beeline history.
   --help                          display this message

   Example:
    1. Connect using simple authentication to HiveServer2 on localhost:10000
    $ beeline -u jdbc:hive2://localhost:10000 username password

    2. Connect using simple authentication to HiveServer2 on hs.local:10000 using -n for username and -p for password
    $ beeline -n username -p password -u jdbc:hive2://hs2.local:10012

    3. Connect using Kerberos authentication with hive/localhost@mydomain.com as HiveServer2 principal
    $ beeline -u "jdbc:hive2://hs2.local:10013/default;principal=hive/localhost@mydomain.com"

    4. Connect using SSL connection to HiveServer2 on localhost at 10000
    $ beeline "jdbc:hive2://localhost:10000/default;ssl=true;sslTrustStore=/usr/local/truststore;trustStorePassword=mytruststorepassword"

    5. Connect using LDAP authentication
    $ beeline -u jdbc:hive2://hs2.local:10013/default <ldap-username> <ldap-password>
    
```