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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VerticaBulkImportActionConfigTest {

  private static final String MOCK_STAGE = "mockStage";
  private static final VerticaImportConfig VALID_CONFIG = new VerticaImportConfig(
    "jdbc:vertica://localhost:5433/test",
    "dbadmin",
    "testpassword",
    "Basic",
    "tableName",
    ",",
    "",
    "/dir/to/file/to/load",
    "false");

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateConnectionStringNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setConnectionString(null)
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(VerticaImportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateConnectionStringEmpty() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setConnectionString("")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(VerticaImportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateIncorrectConnectionString() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setConnectionString("jdbc:connect_my")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(VerticaImportConfig.CONNECTION_STRING));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateBaseLevelAndTableNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setTableName(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.TABLE));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateBaseLevelAndTableEmpty() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setTableName("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.TABLE));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateBaseLevelAndDelimiterNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setDelimiter(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.DELIMITER));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateBaseLevelAndDelimiterEmpty() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setDelimiter("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.DELIMITER));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateAdvancedLevelAndCopyStatementNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setLevel("Advanced")
      .setCopyStatement(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.COPY_STATEMENT));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateAdvancedLevelAndCopyStatementEmpty() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setLevel("Advanced")
      .setCopyStatement("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.COPY_STATEMENT));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserNullAndPasswordNotNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setUser(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.USER, VerticaImportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserEmptyAndPasswordNotNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setUser("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.USER, VerticaImportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserNotNullAndPasswordNull() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setPassword(null)
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.USER, VerticaImportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateUserNotNullAndPasswordEmpty() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setPassword("")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Arrays.asList(VerticaImportConfig.USER, VerticaImportConfig.PASSWORD));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateSeveralFailedConfig() {
    VerticaImportConfig config = VerticaImportConfig.builder(VALID_CONFIG)
      .setTableName(null)
      .setDelimiter(null)
      .build();
    List<List<String>> paramNames = Arrays.asList(
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.TABLE),
      Arrays.asList(VerticaImportConfig.LEVEL, VerticaImportConfig.DELIMITER));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
