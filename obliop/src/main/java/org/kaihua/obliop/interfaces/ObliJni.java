package org.kaihua.obliop.interfaces;

import java.nio.ByteBuffer;

import org.kaihua.obliop.data.JniDataReceiver;

public class ObliJni {

	static {
    System.load("/Users/huahua/Projects/obli_spark/obliop/obliclient/target/x86_64-apple-darwin/debug/libobliclient.dylib");
	}

	private static native String hello(String input);

	public static String doHello(String input) {
		return hello(input);
	}

	static native void doObliDataGet(String uuid, JniDataReceiver jniDataReciver);
}
