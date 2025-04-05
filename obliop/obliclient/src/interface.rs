use oblioplib::data::manager::{get_data_handler, push_data_handler};
use oblioplib::operator::executor::obli_op_ctx_exec;
use proto::protocol::context::*;
use proto::{config, Command};
use std::str;

// pub fn init_session() -> (Sender<Msg>, Receiver<Msg>) {
//     println!("[interface.rs] enter fn init_session");
//     let (tx, rx) = mpsc::channel::<Msg>();
//     let (tx_in, rx_in) = mpsc::channel::<Msg>();
//     thread::spawn(move || {
//       let uuid = Uuid::parse_str(UUID).unwrap();
//       let mut i_ctx = CTX.lock().unwrap();
//       let mut session = i_ctx.open_session(uuid).unwrap();
//       loop {
//         let mut msg = rx.recv().unwrap();
//         // construct param
//         let p0 = ParamTmpRef::new_input(msg.arg_json.as_bytes());

//         match msg.command {
//           Command::DataSend => {
//             let obli_data: ObliData = serde_json::from_str(&msg.arg_json).unwrap();
//             let fbs_buf;
//             unsafe {
//               fbs_buf =
//                 core::slice::from_raw_parts_mut(obli_data.addr as *mut u8, obli_data.length as usize)
//             }
//             let p1 = ParamTmpRef::new_input(fbs_buf);
//             let mut operation = Operation::new(0, p0, p1, ParamNone, ParamNone);
//             // invoke command
//             session
//               .invoke_command(msg.command.into(), &mut operation)
//               .unwrap();
//           }
//           Command::DataGet => {
//             // the data which is fbs
//             let mut out_info = Box::new([0u8; 128]);
//             let p2 = ParamTmpRef::new_output(out_info.as_mut());
//             let mut byt_buf = Box::new([0u8; config::BLOCK_SIZE]);
//             let p3 = ParamTmpRef::new_output(byt_buf.as_mut());
//             let mut operation = Operation::new(0, p0, ParamNone, p2, p3);
//             session.invoke_command(Command::DataGet.into(), &mut operation);
//             // @audit this is tmp patch for remove extra space
//             let max = out_info.iter().filter(|x| **x != 0).count();
//             msg.arg_json = str::from_utf8(out_info[..max].as_ref()).unwrap().to_owned();
//             msg.buf = byt_buf;
//             tx_in.send(msg);
//           }
//           _ => {
//             let mut operation = Operation::new(0, p0, ParamNone, ParamNone, ParamNone);
//             // invoke command
//             session
//               .invoke_command(msg.command.into(), &mut operation)
//               .unwrap();
//           }
//         }
//       }
//       println!("[interface.rs] leave fn init_session thread loop");
//     });
//     (tx, rx_in)
//   }

pub struct Msg {
    command: Command,
    arg_json: String,
    buf: Box<[u8]>,
}

pub fn op_ctx_exec(op_ctx: &mut proto::protocol::context::Context) -> Result<(), &'static str> {
    obli_op_ctx_exec(op_ctx);
    Ok(())
}

pub fn data_send(data: &ObliData) -> Result<(), &'static str> {
    let fbs_buf;
    unsafe { fbs_buf = core::slice::from_raw_parts_mut(data.addr as *mut u8, data.length as usize) }
    push_data_handler(data, fbs_buf);
    Ok(())
}

pub fn data_get(data: &ObliData) -> Option<Box<[u8]>> {
    // the data which is fbs
    let mut out_info = Box::new([0u8; 128]);
    let mut byt_buf = Box::new([0u8; config::BLOCK_SIZE]);
    get_data_handler(data, &mut out_info[..], &mut byt_buf[..]);
    return Some(byt_buf);
}
