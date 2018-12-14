package org.apache.hadoop.hive.ql.plan.axe;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;

@Explain(displayName = "AXE HashTable Sink Operator")
public class AXEHashTableSinkDesc extends HashTableSinkDesc {
  private static final long serialVersionUID = 1L;
  private byte tag;

  public AXEHashTableSinkDesc(MapJoinDesc clone) {
    super(clone);
  }

  public byte getTag() {
    return tag;
  }

  public void setTag(byte tag) {
    this.tag = tag;
  }
}
