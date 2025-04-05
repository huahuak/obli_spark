package org.kaihua.obliop.operator.context;

public class JoinKeyInfo {
  // only equi-join supported for now
  public JoinKeyInfo(int lpos, int rpos) {
    this.lpos = lpos;
    this.rpos = rpos;
  }
  // position
  int lpos;
  int rpos;
}
