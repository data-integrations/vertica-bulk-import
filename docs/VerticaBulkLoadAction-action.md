# VerticaBulkLoadAction Action


Description
-----------
Action that bulk loads into vertica.


Use Case
--------
The action can be used to bulk load data into vertica database.


Properties
----------

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**copyStatement:** Copy statement to bulk load into vertica. This query must use the COPY statement to load data from STDIN. 
Unlike copying from a file on the host, you do not need superuser privileges to copy a stream. 
All your user account needs is INSERT privileges on the target table.

**path:** File directory path from where all the file need to be loaded to vertica.

**connectionString:** JDBC connection string including database name.


Example
-------
This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
It will run an update command to set the price of record with ID 6 to 20.

    {
        "name": "VerticaBulkLoadAction",
        "plugin": {
            "name": "VerticaBulkLoadAction",
            "type": "action",
            "properties": {
                "user": "user123",
                "password": "password-abc",
                "copyStatement": "COPY testTable FROM STDIN DELIMITER ',' DIRECT ENFORCELENGTH",
                "path": "file:///tmp/vertica/",
                "connectionString": "jdbc:vertica://localhost:5433/test"
            }
        }
    }
