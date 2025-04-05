package org.kaihua.obliop.operator.context;

public class SortOrderInfo {
  public SortOrderInfo(int pos, int direction) {
    this.position = pos;
    this.direction = direction;
  }
  // col
  int position;
  // 1 is aescending, -1 is descending
  int direction;
}
