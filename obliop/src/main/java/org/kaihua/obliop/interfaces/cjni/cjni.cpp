#include "org_kaihua_obliop_interfaces_cjni_CJniUtil.h"

JNIEXPORT jlong JNICALL
Java_org_kaihua_obliop_interfaces_cjni_CJniUtil_doGetAddrFromByteBuffer(
    JNIEnv *env, jclass jcls, jobject bytebuffer) {
  void *ptr = env->GetDirectBufferAddress(bytebuffer);
  return (long)ptr;
}