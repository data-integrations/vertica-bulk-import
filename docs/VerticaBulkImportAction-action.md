# VerticaBulkImport Action


Description
-----------
Bulk import contents of multiple files into vertica table.


Use Case
--------
Vertica Bulk Import Action plugin gets executed after successful mapreduce or spark job. It reads all the files in a given directory and bulk imports contents of those files into vertica table.

Properties
----------

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**path:** File directory path from where all the file need to be loaded to vertica.

**level:** Copy statement level. Basic automatically creates copy statement with tableName and delimiter. To use more options please choose Advanced option.

**autoCommit:** Auto commit after every file? Or commit after all the files are loaded? If selected true, commit is applied for every file.

**tableName:** Name of the vertica table where data will be bulk loaded.

**delimiter:** Delimiter in input files. Each delimited values will become columns in specified vertica table.

**copyStatement:** Copy statement to bulk load into vertica. This query must use the COPY statement to load data from STDIN. 
Unlike copying from a file on the host, you do not need superuser privileges to copy a stream. 
All your user account needs is INSERT privileges on the target table.

**connectionString:** JDBC connection string including database name.


Example
-------
This example connects to a vertica database using the specified 'connectionString', which means
it will connect to the 'test' database of a vertica instance running on 'localhost' and bulk load 
contents of all the files under /tmp/vertica/ directory to providede table. This plugin will generate
COPY testTable FROM STDIN DELIMITER ',' copy statement automatically.

    {
        "name": "VerticaBulkImportAction",
        "plugin": {
            "name": "VerticaBulkImportAction",
            "type": "action",
            "properties": {
                "user": "user123",
                "password": "password-abc",
                "path": "file:///tmp/vertica/",
                "level": "Basic",
                "autoCommit": "false",
                "tableName": "testTable",
                "delimiter": ",",
                "connectionString": "jdbc:vertica://localhost:5433/test"
            }
        }
    }
