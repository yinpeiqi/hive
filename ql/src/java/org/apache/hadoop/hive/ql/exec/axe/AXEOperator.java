package org.apache.hadoop.hive.ql.exec.axe;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
class AXEOperator {
  int id;
  private List<Integer> childOps;
  private List<Integer> parentOps;
  private List<Integer> immediateChildOps;
  private List<Integer> immediateParentOps;

  AXEOperator(int id) {
    this.id = id;
  }

  void addImmediateParent(final int child) {
    if (immediateParentOps == null) {
      immediateParentOps = new ArrayList<>();
    }
    immediateParentOps.add(child);
  }

  void addImmediateChild(final int child) {
    if (immediateChildOps == null) {
      immediateChildOps = new ArrayList<>();
    }
    immediateChildOps.add(child);
  }

  void addParent(final int id) {
    if (parentOps == null) {
      parentOps = new ArrayList<>();
    }
    parentOps.add(id);
  }

  void addChild(final int child) {
    if (childOps == null) {
      childOps = new ArrayList<>();
    }
    childOps.add(child);
  }

}
