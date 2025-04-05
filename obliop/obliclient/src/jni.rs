use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jstring};
use jni::JNIEnv;
use proto::protocol::context::*;

use crate::directbuffer::{DIRECT_BUF_MANAGER};
use crate::interface;

/**
 * @author kahua.li
 * @email moflowerlkh@gmail.com
 * @date 2022/12/23
 **/

#[no_mangle]
pub extern "system" fn Java_org_kaihua_obliop_interfaces_ObliJni_hello(
  env: JNIEnv,
  _jclazz: JClass,
  jstr: JString,
) -> jstring {
  let input: String = env
    .get_string(jstr)
    .expect("Couldn't get java string!")
    .into();
  println!("{}", input);

  // Then we have to create a new java string to return. Again, more info
  // in the `strings` module.
  let output = env
    .new_string(format!("Hello, {}!", input))
    .expect("Couldn't create java string!");
  // Finally, extract the raw pointer to return.
  output.into_raw()
}

/*
 * Class:     org_kaihua_obliop_interfaces_ObliJni
 * Method:    doObliDataGet
 * Signature: (Ljava/lang/String;Lorg/kaihua/obliop/data/JniDataReciver;)Ljava/nio/ByteBuffer;
 * (JNIEnv *, jclass, jstring, jobject)
 */
#[no_mangle]
pub extern "system" fn Java_org_kaihua_obliop_interfaces_ObliJni_doObliDataGet(
  env: JNIEnv,
  _jclazz: JClass,
  uuid: JString,
  rcv: JObject,
) {
  println!("\x1b[0;34m");
  println!("// ------------------enter *ObliDataGet()* in rust------------------ //");
  let uuid: String = env.get_string(uuid).unwrap().into();
  let data = &ObliData::empty_with_uuid(&uuid);
  if let Some(mut buf) = interface::data_get(data) {
    let byt_buf = unsafe {
      env
        .new_direct_byte_buffer(buf.as_mut_ptr(), buf.len())
        .unwrap()
    };

    if let Err(e) = env.call_method(
      rcv,
      "set",
      "(Ljava/nio/ByteBuffer;)V",
      &[JValue::from(JObject::from(byt_buf))],
    ) {
      println!("{}", e)
    };
    DIRECT_BUF_MANAGER
      .lock()
      .unwrap()
      .hold(uuid.as_str(), buf)
      .unwrap();
  } else {
    
  };
  println!("\x1b[0m");
}
