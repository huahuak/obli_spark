package org.kaihua.obliop.interfaces.cjni;

import java.nio.ByteBuffer;

public class CJniUtil {

  static {
    System.load("/root/lkh/java/libcjni.so");
  }
  
  private static native long doGetAddrFromByteBuffer(ByteBuffer buf);

  public static long getAddrFromByteBuffer(ByteBuffer buf) {
    return doGetAddrFromByteBuffer(buf);
  }
}
