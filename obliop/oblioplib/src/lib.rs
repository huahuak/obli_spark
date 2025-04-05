// #![feature(map_try_insert)]
// #![feature(total_cmp)]
pub mod data;
pub mod logger;
pub mod operator;

/**
 * @author kahua.li
 * @email moflowerlkh@gmail.com
 * @date 2023/02/06
 **/

#[cfg(test)]
mod tests {
  use proto::protocol::context::{Context, ObliData};

  use crate::{
    data::manager::{get_data_handler, push_data_handler},
    logger::LOGGER,
    operator::executor::obli_op_ctx_exec,
  };

  #[test]
  fn it_works() {
    let data_json =  "{\"name\":\"NOT DEFINE\",\"id\":\"840d3ef9-6165-4cd8-8cf1-4a4f627cfbad\",\"addr\":281473501987684,\"length\":108,\"prepared\":true,\"in_use\":0}";
    let ctx_json = "{\"expressions\":[{\"id\":\"bd054a2a-6f59-42af-9db0-196c24f7057b\",\"typ\":\"HASH\",\"input\":{\"name\":\"NOT DEFINE\",\"id\":\"840d3ef9-6165-4cd8-8cf1-4a4f627cfbad\",\"addr\":281473501987684,\"length\":108,\"prepared\":true,\"in_use\":0},\"output\":{\"name\":\"NOT DEFINE\",\"id\":\"b9e5d25c-32a9-4a89-b215-0b02188c6aa4\",\"addr\":0,\"length\":0,\"prepared\":false,\"in_use\":0},\"children\":[{\"id\":\"f2dcb282-3d6a-470f-9894-39060e231511\",\"typ\":\"MOD\",\"input\":{\"name\":\"NOT DEFINE\",\"id\":\"b9e5d25c-32a9-4a89-b215-0b02188c6aa4\",\"addr\":0,\"length\":0,\"prepared\":false,\"in_use\":0},\"output\":{\"name\":\"NOT DEFINE\",\"id\":\"0d582df9-b61f-4310-95e8-c0f119c51af3\",\"addr\":0,\"length\":0,\"prepared\":false,\"in_use\":0},\"children\":[]}]}]}";
    let target_json = "{\"name\":\"NOT DEFINE\",\"id\":\"b9e5d25c-32a9-4a89-b215-0b02188c6aa4\",\"addr\":281473501987684,\"length\":108,\"prepared\":true,\"in_use\":0}";
    let fbs_buf = [
      4u8, 0, 0, 0, 202, 255, 255, 255, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 218, 255, 255, 255, 4,
      0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 8, 0, 14, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 3, 12, 0, 0,
      0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 30, 0, 0, 0, 104, 101, 108, 108, 111, 32,
      119, 111, 114, 108, 100, 33, 32, 104, 101, 114, 101, 32, 105, 115, 32, 106, 97, 118, 97, 32,
      116, 101, 115, 116, 0, 0,
    ];
    let data: ObliData = serde_json::from_str(data_json).unwrap();
    push_data_handler(&data, &fbs_buf).unwrap();
    let mut ctx: Context = serde_json::from_str(ctx_json).unwrap();
    obli_op_ctx_exec(&mut ctx).unwrap();
    let mut output = [0u8; 1024];
    let mut info = [0u8, 128];
    let target: ObliData = serde_json::from_str(target_json).unwrap();
    get_data_handler(&target, &mut info ,&mut output).unwrap();

    let mut cnt = 0;
    print!("\x1b[0;37m");
    loop {
      match LOGGER.lock().unwrap().next() {
        Some(info) => {
          println!("[{}] {}", cnt, info);
          cnt += 1;
        }
        None => break,
      }
    }
    print!("\x1b[0m");
  }
}
