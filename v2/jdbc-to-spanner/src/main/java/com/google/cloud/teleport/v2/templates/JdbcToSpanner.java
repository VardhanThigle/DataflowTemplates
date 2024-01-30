/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.JdbcToSpannerOptions;
import com.google.cloud.teleport.v2.utils.JdbcConverters;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing Spanner
 * database.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/jdbc-to-googlecloud/README_Jdbc_to_Spanner_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Jdbc_to_Spanner_Flex",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to Spanner",
    description = {
      "The JDBC to Spanner template is a batch pipeline that copies data from a relational"
          + " database into an existing Spanner database. This pipeline uses JDBC to connect to"
          + " the relational database. You can use this template to copy data from any relational"
          + " database with available JDBC drivers into Spanner.",
      "For an extra layer of protection, you can also pass in a Cloud KMS key along with a"
          + " Base64-encoded username, password, and connection string parameters encrypted with"
          + " the Cloud KMS key. See the <a"
          + " href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud"
          + " KMS API encryption endpoint</a> for additional details on encrypting your username,"
          + " password, and connection string parameters."
    },
    optionsClass = JdbcToSpannerOptions.class,
    flexContainerName = "jdbc-to-spanner",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-spanner",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The JDBC drivers for the relational database must be available.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema.",
      "The relational database must be accessible from the subnet where Dataflow runs."
    })
public class JdbcToSpanner {

  private static class JdbcDataSourceProviderFn
      implements org.apache.beam.sdk.transforms.SerializableFunction<Void, javax.sql.DataSource> {
    private JdbcIO.DataSourceConfiguration config;
    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final Lock writeLock = readWriteLock.writeLock();
    private static final Lock readLock = readWriteLock.readLock();
    private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourceProviderFn.class);
    private static transient volatile javax.sql.DataSource dataSource = null;

    JdbcDataSourceProviderFn(JdbcToSpannerOptions options) {
      config = getDataSourceConfiguration(options);
    }

    private javax.sql.DataSource initializeDataSourceIfRequired() {
      // Initialize the connection pool if required.
      javax.sql.DataSource ret = null;
      ret = getDataSource();
      if (ret == null) {
        ret = initializeDataSource();
      }
      return ret;
    }

    private javax.sql.DataSource initializeDataSource() {
      javax.sql.DataSource ret;
      writeLock.lock();
      try {
        // Check if it was already initialized till the write lock was acquired.
        if (dataSource == null) {
          dataSource = JdbcIO.PoolableDataSourceProvider.of(config).apply(null);
          LOG.debug("initializeDataSource dataSource" + dataSource.toString());
        }
        ret = dataSource;
      } finally {
        writeLock.unlock();
      }
      return ret;
    }

    private javax.sql.DataSource getDataSource() {
      javax.sql.DataSource ret = null;
      readLock.lock();
      try {
        ret = this.dataSource;
      } finally {
        readLock.unlock();
      }
      return ret;
    }

    @Override
    public javax.sql.DataSource apply(Void input) {
      return initializeDataSourceIfRequired();
    }

    private static JdbcIO.DataSourceConfiguration getDataSourceConfiguration(
        JdbcToSpannerOptions options) {
      return JdbcIO.DataSourceConfiguration.create(
              StaticValueProvider.of(options.getDriverClassName()),
              maybeDecrypt(options.getConnectionURL(), null))
          .withUsername(maybeDecrypt(options.getUsername(), null))
          .withPassword(maybeDecrypt(options.getPassword(), null))
          .withMaxConnections(options.getMaxConnections())
          .withConnectionProperties(options.getConnectionProperties())
          .withDriverJars(options.getDriverJars());
    }
  }
  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link JdbcToSpanner#run} method to start the pipeline
   * and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line
    JdbcToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcToSpannerOptions.class);

    run(options);
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(JdbcToSpannerOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    Map<String, Set<String>> columnsToIgnore = getColumnsToIgnore(options);
    for (String table : getTables(options)) {
      PCollection<Mutation> rows =
          pipeline.apply(
              "ReadPartitions_" + table, getJdbcReader(table, columnsToIgnore.get(table), options));
      rows.apply("Write_" + table, getSpannerWrite(options));
    }
    return pipeline.run();
  }

  private static List<String> getTables(JdbcToSpannerOptions options) {
    return Arrays.asList(options.getTables().split(","));
  }

  private static Map<String, Set<String>> getColumnsToIgnore(JdbcToSpannerOptions options) {
    String ignoreStr = options.getIgnoreColumns();
    if (ignoreStr == null || ignoreStr.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Set<String>> ignore = new HashMap<>();
    for (String tableColumns : ignoreStr.split(",")) {
      int tableNameIndex = tableColumns.indexOf(':');
      if (tableNameIndex == -1) {
        continue;
      }
      String table = tableColumns.substring(0, tableNameIndex);
      String columnStr = tableColumns.substring(tableNameIndex + 1);
      Set<String> columns = new HashSet<>(Arrays.asList(columnStr.split(";")));
      ignore.put(table, columns);
    }
    return ignore;
  }

  private static JdbcIO.ReadWithPartitions<Mutation, Long> getJdbcReader(
      String table, Set<String> columnsToIgnore, JdbcToSpannerOptions options) {
    return JdbcIO.<Mutation>readWithPartitions()
        .withDataSourceProviderFn(new JdbcDataSourceProviderFn(options))
        .withTable(table)
        .withPartitionColumn(options.getPartitionColumn())
        .withRowMapper(JdbcConverters.getResultSetToMutation(table, columnsToIgnore))
        .withNumPartitions(options.getNumPartitions());
  }

  private static Write getSpannerWrite(JdbcToSpannerOptions options) {
    // SpannerIO internally uses SpannerAccssor, which maintains
    // a single connection with Spanner.
    FailureMode failureMode = (options.getIgnoreInvalidMutations())? FailureMode.REPORT_FAILURES : FailureMode.FAIL_FAST;
    return SpannerIO.write()
        .withFailureMode(failureMode)
        .withProjectId(options.getProjectId())
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getDatabaseId());
  }

}
