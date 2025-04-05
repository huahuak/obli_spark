use std::cmp::Ordering;

use proto::protocol::context::{ObliData, SortOrderInfo};

use crate::data::manager::{Item, Record, DATA_MANAGER};
use crate::data::reader::DataIterator;
use crate::data::writer::DataWriter;

pub fn sort_exec(
  input: &ObliData,
  output: &ObliData,
  ordering: Vec<SortOrderInfo>,
) -> Result<(), &'static str> {
  let iter = &mut DataIterator::new(input);
  let mut data_vec: Vec<Record> = iter.into();
  let mut ans = 0;
  data_vec.sort_by(|a, b| {
    for order in &ordering {
      let a = &a.items[order.position as usize];
      let b = &b.items[order.position as usize];
      match a {
        Item::Int(a) => {
          if let Item::Int(b) = b {
            ans = a - b;
          }
        }
        Item::Str(a) => {
          if let Item::Str(b) = b {
            ans = a.cmp(b) as i32
          }
        }
        Item::Double(a) => {
          if let Item::Double(b) = b {
            ans = a.total_cmp(b) as i32;
          }
        }
      }
      if ans != 0 {
        break;
      }
    }

    if ans > 0 {
      Ordering::Greater
    } else if ans < 0 {
      Ordering::Less
    } else {
      Ordering::Equal
    }
  });

  let mut writer = DataWriter::new();
  data_vec.iter().for_each(|record| writer.write(record));
  let byt_buf = writer.finish();

  let mut dm = DATA_MANAGER.exclusive_access();
  // push fbs buf_result to output data
  if let Some(data) = dm.get_data_mut(&output.id) {
    byt_buf
      .iter()
      .for_each(|item| data.buffer.lock().unwrap().push(*item));
  } else {
    return Err("[operator::hasher::sort_exec()] output data don't exist");
  }
  drop(dm);

  Ok(())
}
