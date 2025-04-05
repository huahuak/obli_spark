package org.kaihua.obliop.interfaces;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.astonbitecode.j4rs.api.Instance;
import org.astonbitecode.j4rs.api.java2rust.Java2RustUtils;
import org.kaihua.obliop.data.JniDataReceiver;
import org.kaihua.obliop.data.ObliData;
import org.kaihua.obliop.data.RetObj;
import org.kaihua.obliop.operator.context.Context;

/**
 * @author kahua.li (moflowerlkh@foxmail.com)
 * @date 2022/11/30
 **/
public class ObliOp {

  static {
    System.load("/Users/huahua/Projects/obli/out/libobliclient.dylib");
  }

  // ------------------ obli op exec ------------------ //
  private static native Instance doObliOpCtxExec(Instance<Context> ctx);

  public static RetObj ObliOpCtxExec(Context ctx) {
    return Java2RustUtils.getObjectCasted(
        doObliOpCtxExec(Java2RustUtils.createInstance(ctx)));
  }

  // ------------------ obli op close ------------------ //
  private static native Instance doObliOpClose(Instance<Integer> opId);

  public static RetObj ObliOpClose(int opId) {
    return Java2RustUtils.getObjectCasted(
        doObliOpClose(Java2RustUtils.createInstance(opId)));
  }

  // ------------------ obli data send------------------ //
  private static native Instance doObliDataSend(Instance<ObliData> data);

  public static RetObj ObliDataSend(ObliData data) {
    return Java2RustUtils.getObjectCasted(
        doObliDataSend(Java2RustUtils.createInstance(data)));
  }

  // ------------------ obli data get ------------------ //
  public static Optional<ByteBuffer> ObliDataGet(ObliData obliData) {
    JniDataReceiver jniDataReceiver = new JniDataReceiver();
    ObliJni.doObliDataGet(obliData.id, jniDataReceiver);
    return jniDataReceiver.get();
  }
}
