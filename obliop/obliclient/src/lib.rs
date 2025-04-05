mod config;
mod interface;
mod jni;
mod util;
mod directbuffer;

use j4rs::prelude::*;
use j4rs::InvocationArg;
use j4rs_derive::*;
use proto::protocol::context::*;

use crate::interface::data_send;

/**
 * @author kahua.li
 * @email moflowerlkh@gmail.com
 * @date 2022/12/21
 **/

#[call_from_java("org.kaihua.obliop.interfaces.ObliOp.doObliOpCtxExec")]
fn obli_op_ctx_exec(ctx_java: Instance) -> Result<Instance, String> {
  println!("\x1b[0;34m");
  println!("// ------------------enter *ObliOpCtxExec()* in rust------------------ //");
  // ------------------------------------ //
  let jvm: Jvm = Jvm::attach_thread().unwrap();
  let mut ctx: Context = jvm.to_rust(ctx_java).unwrap();

  let _ = interface::op_ctx_exec(&mut ctx);
  let ret_obj = RetObj { obli_op_id: 0 };
  // println!("// ------------------leave *ObliOpCtxExec()* in rust------------------ //");
  println!("\x1b[0m");
  let i_ret_obj = InvocationArg::new(&ret_obj, "org.kaihua.obliop.data.RetObj");
  Instance::try_from(i_ret_obj).map_err(|error| format!("{}", error))
}

#[call_from_java("org.kaihua.obliop.interfaces.ObliOp.doObliOpClose")]
fn obli_op_close(i_op_id: Instance) -> Result<Instance, String> {
  println!("\x1b[0;34m");
  println!("// ------------------enter *ObliOpClose()* in rust------------------ //");
  // ------------------------------------ //
  let jvm: Jvm = Jvm::attach_thread().unwrap();
  let op_id: i32 = jvm.to_rust(i_op_id).unwrap();

  let ret_obj = RetObj { obli_op_id: op_id };
  // println!("// ------------------leave *ObliOpClose()* in rust------------------ //");
  println!("\x1b[0m");
  let i_ret_obj = InvocationArg::new(&ret_obj, "org.kaihua.obliop.data.RetObj");
  Instance::try_from(i_ret_obj).map_err(|error| format!("{}", error))
}

#[call_from_java("org.kaihua.obliop.interfaces.ObliOp.doObliDataSend")]
fn obli_data_send(i_obli_data: Instance) -> Result<Instance, String> {
  println!("\x1b[0;34m");
  println!("// ------------------enter *ObliDataSend()* in rust------------------ //");
  let jvm: Jvm = Jvm::attach_thread().unwrap();
  let obli_data: ObliData = jvm.to_rust(i_obli_data).unwrap();

  if config::CLIENT_DEBUG_ENABLE {
    dbg!(&obli_data);
    let bytebuf;
    unsafe {
      bytebuf =
        core::slice::from_raw_parts_mut(obli_data.addr as *mut u8, obli_data.length as usize);
    }
    // let row_table = flatbuffers::root::<RowTable>(bytebuf).unwrap();
    // let value = row_table
    //   .rows()
    //   .unwrap()
    //   .get(0)
    //   .fields()
    //   .unwrap()
    //   .get(0)
    //   .value_as_string_value()
    //   .unwrap()
    //   .value()
    //   .unwrap();
    // println!("[fbs_vec.rs] value is {:?} in rust", value);
  }

  let _result = data_send(&obli_data);

  let ret_obj = RetObj { obli_op_id: 0 };
  // println!("// ------------------leave *ObliDataSend()* in rust------------------ //");
  println!("\x1b[0m");
  let i_ret_obj = InvocationArg::new(&ret_obj, "org.kaihua.obliop.data.RetObj");
  Instance::try_from(i_ret_obj).map_err(|error| format!("{}", error))
}