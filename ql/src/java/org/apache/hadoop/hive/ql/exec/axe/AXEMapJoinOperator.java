package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.MapJoinDesc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class AXEMapJoinOperator extends AXEOperator {
  private Map<String, Map<String, List<String>>> bucketFileNameMapping;
  private String bigTableAlias;
  private String dumpFilePrefix;
  private float hashTableMemory;
  private boolean noOuterJoin;
  private int posBigTable;
  private boolean isBucketMapJoin;
  private boolean isDynamicPartitionHashJoin;
  private int tagLength;
  private Map<Byte, List<AXEExpression>> keys;
  private Map<Byte, List<AXEExpression>> exprs;
  private Map<Integer, String> parentToInput;
  private List<String> outputColumnNames;
  Byte[] tagOrder;
  private Map<Byte, List<AXEExpression>> filters;


  AXEMapJoinOperator(int id) {super(id);}

  public void initialize(final MapJoinDesc desc) {
    outputColumnNames = desc.getOutputColumnNames();
    tagOrder = desc.getTagOrder();
    // FIXME(tatiana): check map join implementation
    bucketFileNameMapping = desc.getAliasBucketFileNameMapping();
    bigTableAlias = desc.getBigTableAlias();
    dumpFilePrefix = desc.getDumpFilePrefix();
    hashTableMemory = desc.getHashTableMemoryUsage();
    noOuterJoin = desc.getNoOuterJoin();
    posBigTable = desc.getPosBigTable();
    isBucketMapJoin = desc.isBucketMapJoin();
    isDynamicPartitionHashJoin = desc.isDynamicPartitionHashJoin();
    tagLength = desc.getTagLength();
    keys = new HashMap<>();
    AXEHTSOperator.SetExprs(keys, desc.getKeys());
    exprs = new HashMap<>();
    AXEHTSOperator.SetExprs(exprs, desc.getExprs());
    parentToInput = desc.getParentToInput();
    filters = new HashMap<>();
    AXEHTSOperator.SetExprs(filters, desc.getFilters());
  }
}
