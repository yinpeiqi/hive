package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AXETable {
  final List<String> hdfsUrl = new ArrayList<>();
  final List<Map<String, String>> partitionCols = new ArrayList<>();
  String inputFormat;
  List<Field> schema;
  @SuppressWarnings("FieldCanBeLocal")
  private String name;

  AXETable(String tableName) {
    name = tableName;
    schema = new ArrayList<>();
  }

  public void setSchema(final List<FieldSchema> allCols) {
    for (FieldSchema field : allCols) {
      schema.add(new Field(field.getName(), field.getType()));
    }
  }

  void addPaths(final Map<Path, PartitionDesc> pathPartitionDescMap) {
    for (Map.Entry<Path, PartitionDesc> pathPartitionDescEntry : pathPartitionDescMap.entrySet()) {
      hdfsUrl.add(pathPartitionDescEntry.getKey().toUri().toString());
      partitionCols.add(pathPartitionDescEntry.getValue().getPartSpec());
    }
  }

  void addPath(final Path path) {
    hdfsUrl.add(path.toUri().toString());
  }

  public void setSchema(final RowSchema rowSchema) {
    for (ColumnInfo columnInfo : rowSchema.getSignature()) {
      if (!columnInfo.isHiddenVirtualCol()) {
        schema.add(new Field(columnInfo.getAlias(), columnInfo.getTypeName()));
      }
    }
  }

  static class Field {
    String name;
    String type;

    Field(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }
}
