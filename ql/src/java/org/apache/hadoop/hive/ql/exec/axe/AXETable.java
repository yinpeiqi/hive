package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;

public class AXETable {
  String inputFormat;
  String hdfsUrl;
  private String name;
  List<Field> schema;

  AXETable(String tableName) {
    name = tableName;
    schema = new ArrayList<>();
  }

  public void setSchema(final List<FieldSchema> allCols) {
    for (FieldSchema field : allCols) {
      schema.add(new Field(field.getName(), field.getType()));
    }
  }

  class Field {
    String name;
    String type;

    Field(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }
}
