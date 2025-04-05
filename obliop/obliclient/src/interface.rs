use oblioplib::data::manager::{get_data_handler, push_data_handler};
use oblioplib::operator::executor::obli_op_ctx_exec;
use proto::protocol::context::*;
use proto::{config, Command};
use std::process::exit;
use std::str;

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
