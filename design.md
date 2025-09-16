# **Design Doc: Decoupling Dataflow Graph Size from Table Count in JDBC Uniform Splitter**

| | |
| :--- | :--- |
| **Author** | Gemini |
| **Date** | 2025-09-15 |
| **Status** | Revised |

## **1. Abstract**

The current JDBC `ReadWithUniformPartitions` transform in the `uniformsplitter` module creates a dedicated set of Dataflow stages for each table it processes. This design leads to a Dataflow graph that scales linearly with the number of tables, making it unsuitable for scenarios with thousands of tables (e.g., 5000), where the graph size becomes unmanageably large and hits Dataflow limits. This document proposes a refactoring of the `uniformsplitter` to decouple the number of graph stages from the number of tables. The core idea is to transform table-specific `PTransform`s into generic ones that operate on a `PCollection` of table-specific data elements, moving the table-aware logic into `DoFn`s. This will result in a constant, table-independent graph size, enabling a single Dataflow job to handle thousands of tables from a **single database** efficiently.

## **2. Background & Problem Statement**

The `ReadWithUniformPartitions` transform is designed to read a single JDBC table by splitting it into uniform ranges for parallel processing. My analysis of the implementation reveals that for each table, the transform creates a unique subgraph of operations, including collation mapping, boundary detection, range counting, and data reading. Each of these transforms is instantiated with table-specific parameters, causing the Dataflow graph to bloat when processing many tables. This approach fails to meet a key customer requirement of processing ~5000 tables in a single job.

The goal is to refactor this implementation so that one set of Dataflow stages can process ranges from any table, making the graph size constant and independent of the number of tables.

## **3. Goals**

*   **Decouple Graph Size:** The number of stages in the Dataflow graph must be constant and independent of the number of tables being processed from a single database.
*   **Preserve Functionality:** The refactoring must not alter the existing logic of range splitting, counting, boundary detection, or data reading.
*   **No Performance Degradation:** The performance of processing a single table must not be negatively impacted.
*   **Scalability:** The new design must support processing thousands of tables from a single database in a single Dataflow job.
*   **Extensibility:** The design should provide a clear path to support reading from multiple databases in the future.

## **4. Non-Goals**

*   Adding new features or splitting algorithms to the uniform splitter.
*   Supporting reading from multiple, heterogeneous databases in this milestone.
*   Changing the public-facing API of the parent `SourceDbToSpanner` template in a breaking way.

## **5. Proposed Design**

The core principle is to **lift table-specific configuration from `PTransform`s into data elements within a `PCollection`**. We will create a single, generic processing graph that operates on elements containing table-specific context. For this milestone, the graph will be configured for a single database.

### **5.1. Data Model Changes**

1.  **`TableIdentifier` (New Class):** To provide a path for future cross-database support, we will encapsulate the table identifier in a new, extensible class.
    ```java
    @AutoValue
    public abstract class TableIdentifier implements Serializable {
      public abstract String tableName();
      // Future: public abstract String databaseIdentifier();

      public static TableIdentifier of(String tableName) {
        return new AutoValue_TableIdentifier(tableName);
      }
    }
    ```

2.  **`TableSplitRequest` (New Class):** This class will encapsulate the information required to process one table. Note that database-level configuration is now outside this class.
    ```java
    @AutoValue
    public abstract class TableSplitRequest implements Serializable {
      abstract TableIdentifier tableReference();
      abstract ImmutableList<PartitionColumn> partitionColumns();
      abstract Long approxTotalRowCount();
      abstract JdbcIO.RowMapper<T> rowMapper();
      // ... other table-specific options
    }
    ```

3.  **`Range` & `ColumnForBoundaryQuery` (Modification):** These classes will be modified to hold a reference to their corresponding `TableSplitRequest`.
    ```java
    // Example modification for Range
    @AutoValue
    public abstract class Range implements Serializable {
      // ... existing fields
      abstract TableSplitRequest tableSplitRequest();
    }
    ```

### **5.2. New Top-Level Transform: `MultiTableReadWithUniformPartitions`**

This new `PTransform` will be configured for a single database and will process a list of tables within it.

```java
@AutoValue
public abstract class MultiTableReadWithUniformPartitions<T> extends PTransform<PBegin, PCollection<T>> {

  // DB-level configuration
  abstract UniformSplitterDBAdapter dbAdapter();
  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  // Table-level configuration
  abstract ImmutableList<TableSplitRequest> tableSplitRequests();

  @Override
  public PCollection<T> expand(PBegin input) {
    // 1. Create initial PCollection of requests
    PCollection<TableSplitRequest> requests = input.apply("CreateRequests", Create.of(tableSplitRequests()));

    // 2. Centralized Collation Mapping (DB-level)
    PCollectionView<Map<CollationReference, CollationMapper>> collationView =
        requests.apply("SetupCollationMapping", new CentralizedCollationMapperTransform(dbAdapter(), dataSourceProviderFn()));

    // 3. Initial Split
    PCollection<Range> initialRanges = requests.apply("InitialSplit", new GenericInitialSplit(collationView));

    // 4. Staged Splitting Loop
    PCollection<Range> splitRanges = initialRanges;
    for (int i = 0; i < GLOBAL_SPLIT_STAGE_COUNT; i++) {
      splitRanges = splitRanges.apply("Stage" + i, new GenericSplitStage(collationView, dbAdapter(), dataSourceProviderFn()));
    }

    // 5. Merge Ranges
    PCollection<Range> mergedRanges = splitRanges.apply("MergeRanges", new GenericMergeRanges(collationView));

    // 6. Read Data
    PCollection<T> records = mergedRanges.apply("ReadData", new GenericRangeReader<T>(dbAdapter(), dataSourceProviderFn()));

    return records;
  }
}
```

### **5.3. Generic `DoFn` Implementation**

All `DoFn`s within the generic transforms will now receive the `dbAdapter` and `dataSourceProviderFn` during their construction. They will extract table-specific details from the `Range` object they process.

**Example: `ReadRangeDoFn`**

```java
class ReadRangeDoFn<T> extends DoFn<Range, T> {
  private final UniformSplitterDBAdapter dbAdapter;
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
  // ...

  @ProcessElement
  public void processElement(@Element Range range, OutputReceiver<T> out) {
    TableSplitRequest request = range.tableSplitRequest();
    String tableName = request.tableReference().tableName();
    JdbcIO.RowMapper<T> rowMapper = request.rowMapper();

    // Get a connection from the single DataSource
    try (Connection conn = dataSourceProviderFn.apply(null).getConnection()) {
      // Build the read query dynamically using the single dbAdapter
      String query = dbAdapter.getReadQuery(tableName, ...);
      // ... execute query and map results
    }
  }
}
```
This pattern ensures that while the graph stages are generic, the execution within them is dynamically tailored to each table from the single source database.

### **5.4. Future Enhancement: Supporting Multiple Databases**

This design can be extended to support multiple databases with minimal changes to the core logic.

1.  **`TableIdentifier` Enhancement:** The `TableIdentifier` class would be updated to include a database identifier. This identifier could be a key to a map of `DataSource` providers.
    ```java
    @AutoValue
    public abstract class TableIdentifier implements Serializable {
      public abstract String tableName();
      public abstract String databaseId(); // e.g., "primary-db", "secondary-db"
    }
    ```
2.  **Pipeline Input:** The pipeline would take a `Map<String, DataSourceProvider>` as input, mapping the `databaseId` to its connection provider.
3.  **Grouping:** Before the main processing logic, a `GroupByKey` transform would be applied to the `PCollection<TableSplitRequest>` using `databaseId` as the key. This would create separate collections for each database.
4.  **Processing:** The generic splitting and reading logic would be applied to each of these per-database collections. This could be done by iterating through the grouped collections and applying the `MultiTableReadWithUniformPartitions` transform to each, or by modifying the `DoFn`s to perform a dynamic lookup of the correct `DataSource` based on the `databaseId` within each element.

This phased approach allows us to solve the immediate scalability problem for a single database while establishing a clear and robust path toward multi-database support.

## **6. Testing Plan**

The testing plan remains the same, with an emphasis on ensuring the single-database constraint is respected and that the `TableIdentifier` is correctly propagated.

1.  **Unit Tests:** Refactor existing unit tests for `DoFn`s to pass the new table-aware data objects.
2.  **Transform-Level Tests:** Write tests for the new generic `PTransform`s.
3.  **Integration Test:** Create an integration test to read from 3-5 tables from a single live test database.
4.  **Scalability Test:** A load test will be executed with >200 tables from a single database to validate that the Dataflow graph size remains small and constant.