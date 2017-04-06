Vertica Bulk Import
====================

Vertica Bulk Import Action plugin gets executed after successful mapreduce or spark job. It reads all the files in a given directory and bulk imports contents of those files into vertica table. 

Plugin Configuration
---------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Username** | **N** | N/A | This configuration specifies user identity for connecting to the specified database. Required for databases that need authentication. Optional for databases that do not require authentication. |
| **Password** | **N** | N/A | Specifies password to use to connect to the specified database. Required for databases that need authentication. Optional for databases that do not require authentication. |
| **File Path** | **Y** | N/A | Specifies directory or file path which needs to be loaded to database. |
| **Copy Statement level** | **Y** | Basic| This configuration specifies Copy statement level used by the plugin. If Basic is selected, copy statement will be generated automatically. Advanced option takes whole copy statement. |
| **Auto commit after each file?** | **Y** | false | This configuration specifies if commit needs to happen after every file from the directory or not. If specified false, commit will be applied after all the files are loaded. If specified true, it will be applied after each file. |
| **Vertica Table name** | **N** | N/A | This configuration provides vertica table name to which data will be loaded. Table in vertica must exist. Only works with Basic Copy Statement Level. |
| **Delimiter for the input file** | **N** | , (comma) | Specifies delimiter in the input file. Only works with Basic Copy Statement Level. |
| **Copy Statement** | **N** | N/A | Specifies copy statement for vertica bulk load. Only works with Advanced Copy Statement level. |
| **Connection String** | **Y** | N/A | JDBC connection string including database name. |


Usage Notes
-----------

The plugin can be configured to a read single file or multiple files from a configured HDFS directory and bulk load it into a Vertica table. The plugin uses the capabilities of Vertica to load the data from HDFS into Vertica. The command to load are issued through a Vertica JDBC driver. Vertica's java api `VerticaCopyStream` is then used to write contents of the file as stdin `stream` to vertica table. 

For every load, the plugin starts up a transactions and the transaction is committed only when all the files have been successfully loaded into Vertica. In case of any failures while loading, the transaction is aborted. It's important to note that this will increase the load throughput, but in case of any issues it will rollback the complete fileset. Hence, the plugin provides the ability to commit transaction after every file being loaded into Vertica.

Plugin provides two different ways for loading in bulk to Vertica -- first uses a standard simple approach for loading in delimiter separated files, while the advanced option allows you to specify the ```COPY``` query to load the data. More information about Vertica ```COPY``` command can be found [here](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/AdministratorsGuide/BulkLoadCOPY/BulkLoadingData.htm). This advanced option should be used when you need advanced optimizations.

This plugin emits metrics ```num.of.rows.rejected``` for number of rows successfully loaded and ```num.of.rows.inserted``` number of rows rejected by Vertica bulk load.. 
