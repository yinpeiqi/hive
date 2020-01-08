package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;

public class AXETable {
  final List<String> hdfsUrl = new ArrayList<>();
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

  void addPaths(final List<Path> aliasPaths) {
    for (Path path : aliasPaths) {
      hdfsUrl.add(path.toUri().toString());
    }
  }

  void addPath(final Path path) {
    hdfsUrl.add(path.toUri().toString());
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
