package org.kaihua.obliop.data;

import java.util.UUID;

public class ObliData {
  // @add pulbic for serialization
  public String name = "NOT DEFINE";
  // @todo id can't be random for tolerance
  public String id = UUID.randomUUID().toString();
  public long addr;
  public long length;

  public boolean prepared;
  public int in_use;

  private ObliData() {
    this.prepared = false;
    this.in_use = 0;
  };

  public ObliData(long addr, long length) {
    this();
    this.addr = addr;
    this.length = length;
    this.prepared = true;
  }

  public static ObliData empty() {
    return new ObliData();
  }

  public ObliData setName(String name) {
    this.name = name;
    return this;
  }

  public void releaseDirectMemory() {
    
  }
}