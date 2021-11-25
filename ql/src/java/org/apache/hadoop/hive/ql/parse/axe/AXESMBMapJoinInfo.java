package org.apache.hadoop.hive.ql.parse.axe;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.MapWork;

import java.util.ArrayList;
import java.util.List;

class AXESMBMapJoinInfo {
  Operator<?> bigTableRootOp;
  List<Operator<?>> smallTableRootOps = new ArrayList<>();
  MapWork mapWork;
}
