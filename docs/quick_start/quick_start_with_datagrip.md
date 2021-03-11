# 5.Getting Started With Kyuubi and DataGrip
## 5.1 what is DataGrip
DataGrip is a multi-engine database environment released by JetBrains, supporting MySQL and PostgreSQL, Microsoft SQL Server and Oracle, Sybase, DB2, SQLite, HyperSQL, Apache Derby and H2.

[About DataGrip](https://www.jetbrains.com/datagrip/)
## 5.2 Preparation
### 5.2.1 Get DataGrip And Install
Please go to [Download DataGrip](https://www.jetbrains.com/datagrip/download) to get and install an appropriate version for yourself.
### 5.2.2 Get Kyuubi Started
[Get kyuubi server started](https://kyuubi.readthedocs.io/en/latest/quick_start/quick_start.html) before you try DataGrip with kyuubi.

For debugging purpose, you can use ```tail -f``` or ```tailf``` to track the server log.
## 5.3 Configurations
### 5.3.1 Start DataGrip
After you install DataGrip, just launch it.
### 5.3.2 Select Database
Substantially, this step is to choose a JDBC Driver type to use later. We can choose Apache Hive to set up a driver for Kyuubi.

![select database](../imgs/dataGrip/select_database.png)
### 5.3.3 Datasoure Driver
You should first download missing driver files, just click on the link below, DataGrip will download and install those.
![datasource and driver](../imgs/dataGrip/datasource_and_driver.png)
### 5.3.4 Generic JDBC Connection Settings
After install dirver, you should configure the right host and port which you can find in kyuubi server log.By default, we use ```localhost``` and ```10009``` to config.

And alse, you can fill other config.

After generic config, you can use test connection to test.

![configuration](../imgs/dataGrip/configuration.png)
## 5.4 Interacting With Kyuubi server
Now, you can interacing with kyuubi server

The left side of the photo is the table, and the right side of the photo is the console.

You can interact through the visual interface or code

![workspace](../imgs/dataGrip/workspace.png)
## 5.5 Epilogue
There are many other amazing features in both Kyuubi and DBeaver and here is just the tip of the iceberg. The rest is for you to discover.