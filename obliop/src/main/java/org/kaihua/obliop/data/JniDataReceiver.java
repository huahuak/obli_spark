package org.kaihua.obliop.data;

import java.nio.ByteBuffer;
import java.util.Optional;

public class JniDataReceiver {
  private ByteBuffer bytBuf;

  public Optional<ByteBuffer> get() {
    if (bytBuf == null) {
      return Optional.empty();
    }
    return Optional.of(bytBuf);

  }

  public void set(ByteBuffer buf) {
    this.bytBuf = buf;
  }
}
