Vertica Bulk Load
=================

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=vertica-bulk-load"/></a> [![Build Status](https://travis-ci.org/hydrator/vertica-bulk-load.svg?branch=master)](https://travis-ci.org/hydrator/vertica-bulk-load) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img alt="CDAP Action" src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/> []() <img src="https://cdap-users.herokuapp.com/assets/cm-available.svg"/>

Vertica Bulk Load Action plugin gets executed after successful mapreduce or spark job. It reads all the files in a given directory and bulk loads all the data from those files into vertica table. 

<img align="center" src="docs/plugin-vertica-bulk-load.png"  width="400" alt="plugin configuration" />

Usage Notes
-----------

This plugin can be configured to read all the contenets for multiple files in a directory and bulk load into vertica table. This plugin provides option to commit after every file written to vertica table. 

If Basic level of Copy statement is selected, table name and delimiter should be provided. With Advanced option selected, table name and delimiter fileds will not have any effect.

This plugin also emits metrics "num.of.rows.rejected"" for number of rows successfully created and "num.of.rows.inserted" number of rows rejected. 

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
| **Delimiter for the input file** | **N** | , | Specifies delimiter in the input file. Only works with Basic Copy Statement Level. |
| **Copy Statement** | **N** | N/A | Specifies copy statement for vertica bulk load. Only works with Advanced Copy Statement level. |
| **Connection String** | **Y** | N/A | JDBC connection string including database name. |

Build
-----
To build this plugin:

```
   mvn clean package
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/vertica-bulk-load-<version>.jar config-file <target/vertica-bulk-load-<version>.json>

For example, if your artifact is named 'vertica-bulk-load-<version>':

    > load artifact target/vertica-bulk-load-<version>.jar config-file target/vertica-bulk-load-<version>.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.  