/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.source;

import org.apache.gluten.TestConfUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSparkDataFile {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withZone()),
          required(109, "tsntz", Types.TimestampType.withoutZone()),
          required(110, "s", Types.StringType.get()),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // maximum precision
          );
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA)
          .identity("b")
          .bucket("i", 2)
          .identity("l")
          .identity("f")
          .identity("d")
          .identity("date")
          .hour("ts")
          .identity("ts")
          .identity("tsntz")
          .truncate("s", 2)
          .identity("bytes")
          .bucket("dec_9_0", 2)
          .bucket("dec_11_2", 2)
          .bucket("dec_38_10", 2)
          .build();

  private static SparkSession spark;
  private static JavaSparkContext sparkContext = null;

  @BeforeClass
  public static void startSpark() {
    TestSparkDataFile.spark =
        SparkSession.builder().master("local[2]").config(TestConfUtil.GLUTEN_CONF).getOrCreate();
    TestSparkDataFile.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDataFile.spark;
    TestSparkDataFile.spark = null;
    TestSparkDataFile.sparkContext = null;
    currentSpark.stop();
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testValueConversion() throws IOException {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);
    checkSparkContentFiles(table);
  }

  @Test
  public void testValueConversionPartitionedTable() throws IOException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
    checkSparkContentFiles(table);
  }

  @Test
  public void testValueConversionWithEmptyStats() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);
    checkSparkContentFiles(table);
  }

  private void checkSparkContentFiles(Table table) throws IOException {
    Iterable<InternalRow> rows = RandomData.generateSpark(table.schema(), 200, 0);
    JavaRDD<InternalRow> rdd = sparkContext.parallelize(Lists.newArrayList(rows));
    Dataset<Row> df =
        spark.internalCreateDataFrame(
            JavaRDD.toRDD(rdd), SparkSchemaUtil.convert(table.schema()), false);

    df.write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();

    PartitionSpec dataFilesSpec = table.spec();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    List<DataFile> dataFiles = Lists.newArrayList();
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io())) {
      for (DataFile dataFile : reader) {
        checkDataFile(dataFile.copy(), DataFiles.builder(dataFilesSpec).copy(dataFile).build());
        dataFiles.add(dataFile.copy());
      }
    }

    UpdatePartitionSpec updateSpec = table.updateSpec();
    for (PartitionField field : dataFilesSpec.fields()) {
      updateSpec.removeField(field.name());
    }
    updateSpec.commit();

    List<DeleteFile> positionDeleteFiles = Lists.newArrayList();
    List<DeleteFile> equalityDeleteFiles = Lists.newArrayList();

    RowDelta rowDelta = table.newRowDelta();

    for (DataFile dataFile : dataFiles) {
      DeleteFile positionDeleteFile = createPositionDeleteFile(table, dataFile);
      positionDeleteFiles.add(positionDeleteFile);
      rowDelta.addDeletes(positionDeleteFile);
    }

    DeleteFile equalityDeleteFile1 = createEqualityDeleteFile(table);
    equalityDeleteFiles.add(equalityDeleteFile1);
    rowDelta.addDeletes(equalityDeleteFile1);

    DeleteFile equalityDeleteFile2 = createEqualityDeleteFile(table);
    equalityDeleteFiles.add(equalityDeleteFile2);
    rowDelta.addDeletes(equalityDeleteFile2);

    rowDelta.commit();

    Dataset<Row> dataFileDF = spark.read().format("iceberg").load(tableLocation + "#data_files");
    List<Row> sparkDataFiles = shuffleColumns(dataFileDF).collectAsList();
    assertThat(sparkDataFiles).hasSameSizeAs(dataFiles);

    Types.StructType dataFileType = DataFile.getType(dataFilesSpec.partitionType());
    StructType sparkDataFileType = sparkDataFiles.get(0).schema();
    SparkDataFile dataFileWrapper = new SparkDataFile(dataFileType, sparkDataFileType);

    for (int i = 0; i < dataFiles.size(); i++) {
      checkDataFile(dataFiles.get(i), dataFileWrapper.wrap(sparkDataFiles.get(i)));
    }

    Dataset<Row> positionDeleteFileDF =
        spark.read().format("iceberg").load(tableLocation + "#delete_files").where("content = 1");
    List<Row> sparkPositionDeleteFiles = shuffleColumns(positionDeleteFileDF).collectAsList();
    assertThat(sparkPositionDeleteFiles).hasSameSizeAs(positionDeleteFiles);

    Types.StructType positionDeleteFileType = DataFile.getType(dataFilesSpec.partitionType());
    StructType sparkPositionDeleteFileType = sparkPositionDeleteFiles.get(0).schema();
    SparkDeleteFile positionDeleteFileWrapper =
        new SparkDeleteFile(positionDeleteFileType, sparkPositionDeleteFileType);

    for (int i = 0; i < positionDeleteFiles.size(); i++) {
      checkDeleteFile(
          positionDeleteFiles.get(i),
          positionDeleteFileWrapper.wrap(sparkPositionDeleteFiles.get(i)));
    }

    Dataset<Row> equalityDeleteFileDF =
        spark.read().format("iceberg").load(tableLocation + "#delete_files").where("content = 2");
    List<Row> sparkEqualityDeleteFiles = shuffleColumns(equalityDeleteFileDF).collectAsList();
    assertThat(sparkEqualityDeleteFiles).hasSameSizeAs(equalityDeleteFiles);

    Types.StructType equalityDeleteFileType = DataFile.getType(table.spec().partitionType());
    StructType sparkEqualityDeleteFileType = sparkEqualityDeleteFiles.get(0).schema();
    SparkDeleteFile equalityDeleteFileWrapper =
        new SparkDeleteFile(equalityDeleteFileType, sparkEqualityDeleteFileType);

    for (int i = 0; i < equalityDeleteFiles.size(); i++) {
      checkDeleteFile(
          equalityDeleteFiles.get(i),
          equalityDeleteFileWrapper.wrap(sparkEqualityDeleteFiles.get(i)));
    }
  }

  private Dataset<Row> shuffleColumns(Dataset<Row> df) {
    List<Column> columns =
        Arrays.stream(df.columns()).map(ColumnName::new).collect(Collectors.toList());
    Collections.shuffle(columns);
    return df.select(columns.toArray(new Column[0]));
  }

  private void checkDataFile(DataFile expected, DataFile actual) {
    assertThat(expected.equalityFieldIds()).isNull();
    assertThat(actual.equalityFieldIds()).isNull();
    checkContentFile(expected, actual);
    checkStructLike(expected.partition(), actual.partition());
  }

  private void checkDeleteFile(DeleteFile expected, DeleteFile actual) {
    assertThat(expected.equalityFieldIds()).isEqualTo(actual.equalityFieldIds());
    checkContentFile(expected, actual);
    checkStructLike(expected.partition(), actual.partition());
  }

  private void checkContentFile(ContentFile<?> expected, ContentFile<?> actual) {
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.path()).isEqualTo(expected.path());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    assertThat(actual.upperBounds()).isEqualTo(expected.upperBounds());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
  }

  private void checkStructLike(StructLike expected, StructLike actual) {
    assertThat(actual.size()).isEqualTo(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      assertThat(actual.get(i, Object.class)).isEqualTo(expected.get(i, Object.class));
    }
  }

  private DeleteFile createPositionDeleteFile(Table table, DataFile dataFile) {
    PartitionSpec spec = table.specs().get(dataFile.specId());
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath("/path/to/pos-deletes-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(dataFile.fileSizeInBytes() / 4)
        .withPartition(dataFile.partition())
        .withRecordCount(2)
        .withMetrics(
            new Metrics(
                2L,
                null, // no column sizes
                null, // no value counts
                null, // no null counts
                null, // no NaN counts
                ImmutableMap.of(
                    MetadataColumns.DELETE_FILE_PATH.fieldId(),
                    Conversions.toByteBuffer(Types.StringType.get(), dataFile.path())),
                ImmutableMap.of(
                    MetadataColumns.DELETE_FILE_PATH.fieldId(),
                    Conversions.toByteBuffer(Types.StringType.get(), dataFile.path()))))
        .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(35))
        .build();
  }

  private DeleteFile createEqualityDeleteFile(Table table) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofEqualityDeletes(3, 4)
        .withPath("/path/to/eq-deletes-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(250)
        .withRecordCount(1)
        .withSortOrder(SortOrder.unsorted())
        .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(35))
        .build();
  }
}
