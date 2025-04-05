package org.kaihua.obliop.util;

import java.util.Base64;

public class Util {
  public static void printByte(byte[] byt) {
    System.out.println("// ------------------ print byte ------------------ //");
    int cnt = 0;
    byte[] encode = Base64.getEncoder().encode(byt);
    // System.out.println(new String(encode));
    for (byte c : byt) {
      System.out.print(String.format("%02x", c) + ' ');
      cnt += 1;
      if (cnt == 16) {
        System.out.println();
        cnt = 0;
      }
    }
    System.out.println();
    for (byte c : byt) {
      System.out.print((char) c);
    }
    System.out.println();
    System.out.println("// ------------------ print byte ------------------ //");
  }

}
