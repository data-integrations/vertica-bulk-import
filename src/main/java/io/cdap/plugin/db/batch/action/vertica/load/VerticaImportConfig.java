/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.db.batch.action.vertica.load;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Vertica Import config
 */
public class VerticaImportConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String COPY_STATEMENT = "copyStatement";
  public static final String PATH = "path";
  public static final String LEVEL = "level";
  public static final String TABLE = "tableName";
  public static final String DELIMITER = "delimiter";
  public static final String AUTO_COMMIT = "autoCommit";

  private static final String CONNECTION_STRING_PREFIX = "jdbc:vertica://";

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string including database name.")
  @Macro
  private String connectionString;

  @Name(USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String password;

  @Name(LEVEL)
  @Description("Copy statement query level. Basic automatically creates copy statement with tableName and delimiter. " +
    "To use more options please choose Advanced option.")
  @Macro
  private String level;

  @Name(TABLE)
  @Description("Name of the vertica table where data will be bulk loaded.")
  @Nullable
  @Macro
  private String tableName;

  @Name(DELIMITER)
  @Description("Delimiter in input files. Each delimited values will become columns in specified vertica table")
  @Nullable
  @Macro
  private String delimiter;

  @Name(COPY_STATEMENT)
  @Description("Copy statement to bulk load into vertica. This query must use the COPY statement to load data from " +
    "STDIN. Unlike copying from a file on the host, you do not need superuser privileges to copy a stream. All " +
    "your user account needs is INSERT privileges on the target table.")
  @Macro
  @Nullable
  private String copyStatement;

  @Name(PATH)
  @Description("File directory path from where all the file need to be loaded to vertica.")
  @Macro
  private String path;

  @Name(AUTO_COMMIT)
  @Description("Auto commit after every file? Or commit after all the files are loaded? If selected true, commit is" +
    " applied for every file.")
  private String autoCommit;

  public VerticaImportConfig(String connectionString, String user, String password, String level, String tableName,
                             String delimiter, String copyStatement, String path, String autoCommit) {
    this.connectionString = connectionString;
    this.user = user;
    this.password = password;
    this.level = level;
    this.tableName = tableName;
    this.delimiter = delimiter;
    this.copyStatement = copyStatement;
    this.path = path;
    this.autoCommit = autoCommit;
  }

  private VerticaImportConfig(Builder builder) {
    connectionString = builder.connectionString;
    user = builder.user;
    password = builder.password;
    level = builder.level;
    tableName = builder.tableName;
    delimiter = builder.delimiter;
    copyStatement = builder.copyStatement;
    path = builder.path;
    autoCommit = builder.autoCommit;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(VerticaImportConfig copy) {
    return builder()
      .setConnectionString(copy.connectionString)
      .setUser(copy.user)
      .setPassword(copy.password)
      .setLevel(copy.level)
      .setTableName(copy.tableName)
      .setDelimiter(copy.delimiter)
      .setCopyStatement(copy.copyStatement)
      .setPath(copy.path)
      .setAutoCommit(copy.autoCommit);
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public String getLevel() {
    return level;
  }

  @Nullable
  public String getTableName() {
    return tableName;
  }

  @Nullable
  public String getDelimiter() {
    return delimiter;
  }

  @Nullable
  public String getCopyStatement() {
    return copyStatement;
  }

  public String getPath() {
    return path;
  }

  public String getAutoCommit() {
    return autoCommit;
  }

  public void validate(FailureCollector failureCollector) {
    if(!containsMacro(CONNECTION_STRING)) {
      if (Strings.isNullOrEmpty(connectionString)) {
        failureCollector.addFailure(
          "Connection String must be provided.", null)
          .withConfigProperty(CONNECTION_STRING);
      } else if (!connectionString.startsWith(CONNECTION_STRING_PREFIX)) {
        failureCollector.addFailure(
          "Invalid connection string.",
          "Connection String must comply with format - jdbc:vertica://<VerticaHost>:<portNumber>/<databaseName>")
          .withConfigProperty(CONNECTION_STRING);
      }
    }
    if (!containsMacro(USER) && !containsMacro(PASSWORD)) {
      if (Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(password)) {
        failureCollector.addFailure(
          "Username is not specified.",
          "Ensure both username and password are provided.")
          .withConfigProperty(USER).withConfigProperty(PASSWORD);
      }
      if (!Strings.isNullOrEmpty(user) && Strings.isNullOrEmpty(password)) {
        failureCollector.addFailure(
          "Password is not specified.",
          "Ensure both username and password are provided.")
          .withConfigProperty(USER).withConfigProperty(PASSWORD);
      }
    }
    if (!containsMacro(LEVEL)) {
      if (!containsMacro(TABLE) && level.equalsIgnoreCase("basic")) {
        if (Strings.isNullOrEmpty(tableName)) {
          failureCollector.addFailure(
            "Vertica Table name is not specified.",
            "Ensure vertica table name is specified for basic level.")
            .withConfigProperty(LEVEL).withConfigProperty(TABLE);
        }

        if (!containsMacro(DELIMITER) && Strings.isNullOrEmpty(delimiter)) {
          failureCollector.addFailure(
            "Delimiter for the input file is not specified.",
            "Ensure delimiter is provided for basic level.")
            .withConfigProperty(LEVEL).withConfigProperty(DELIMITER);
        }
      } else {
        if (!containsMacro(COPY_STATEMENT) && Strings.isNullOrEmpty(copyStatement)) {
          failureCollector.addFailure(
            "Copy Statement is not specified.",
            "Ensure a valid copy statement is provided for advanced level.")
            .withConfigProperty(LEVEL).withConfigProperty(COPY_STATEMENT);
        }
      }
    }
  }

  /**
   * Builder for creating a {@link VerticaImportConfig}.
   */
  public static final class Builder {
    private String connectionString;
    private String user;
    private String password;
    private String level;
    private String tableName;
    private String delimiter;
    private String copyStatement;
    private String path;
    private String autoCommit;

    private Builder() {
    }

    public Builder setConnectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setLevel(String level) {
      this.level = level;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder setCopyStatement(String copyStatement) {
      this.copyStatement = copyStatement;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setAutoCommit(String autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public VerticaImportConfig build() {
      return new VerticaImportConfig(this);
    }
  }
}
