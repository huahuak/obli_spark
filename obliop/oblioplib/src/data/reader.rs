use std::{
  ptr::read,
  sync::{Arc, Mutex},
};

use proto::{
  collection::vector_generated::org::kaihua::obliop::collection::fbs::{FieldUnion, RowTable},
  protocol::context::ObliData,
};

use super::manager::{Item, Record, DATA_MANAGER};

pub struct DataIterator {
  buf: Arc<Mutex<Vec<u8>>>,
  pub cur: usize,
  size: usize,
}

impl DataIterator {
  pub fn new(data: &ObliData) -> DataIterator {
    let dm = DATA_MANAGER.exclusive_access();
    let buf = Arc::clone(&dm.get_data(&data.id).unwrap().buffer);
    let size = flatbuffers::root::<RowTable>(buf.lock().unwrap().as_ref())
      .unwrap()
      .rows()
      .unwrap()
      .len();
    let iter = DataIterator { buf, cur: 0, size };
    drop(dm);
    iter
  }
}

impl Iterator for DataIterator {
  type Item = Record;

  fn next(&mut self) -> Option<Self::Item> {
    if self.cur >= self.size {
      return None;
    }
    let buf = self.buf.lock().unwrap();
    let row_table = flatbuffers::root::<RowTable>(buf.as_ref())
      .unwrap()
      .rows()
      .unwrap();
    let mut ret = Record { items: vec![] };
    for field in row_table.get(self.cur).fields().unwrap() {
      match field.value_type() {
        FieldUnion::IntValue => {
          let i = field.value_as_int_value().unwrap().value();
          ret.items.push(Item::Int(i))
        }
        FieldUnion::StringValue => {
          let s = field.value_as_string_value().unwrap().value().unwrap();
          ret.items.push(Item::Str(String::from(s)));
        }
        FieldUnion::DoubleValue => {
          let f = field.value_as_double_value().unwrap().value();
          ret.items.push(Item::Double(f));
        }
        _ => {
          panic!("[ta::getter.rs::next()] type not found implement")
        }
      }
    }
    self.cur += 1;
    Some(ret)
  }
}

impl From<&mut DataIterator> for Vec<Record> {
  fn from(iter: &mut DataIterator) -> Self {
    let mut all = vec![];

    let mut cur = 0;
    let size = iter.size;
    let buf = iter.buf.lock().unwrap();
    let row_table = flatbuffers::root::<RowTable>(buf.as_ref())
      .unwrap()
      .rows()
      .unwrap();
    while cur < size {
      let mut ret = Record { items: vec![] };
      for field in row_table.get(cur).fields().unwrap() {
        match field.value_type() {
          FieldUnion::IntValue => {
            let i = field.value_as_int_value().unwrap().value();
            ret.items.push(Item::Int(i))
          }
          FieldUnion::StringValue => {
            let s = field.value_as_string_value().unwrap().value().unwrap();
            ret.items.push(Item::Str(String::from(s)));
          }
          FieldUnion::DoubleValue => {
            let f = field.value_as_double_value().unwrap().value();
            ret.items.push(Item::Double(f));
          }
          _ => {
            panic!("[ta::getter.rs::next()] type not found implement")
          }
        }
      }
      cur += 1;
      all.push(ret);
    }

    all
  }
}

impl Clone for DataIterator {
  fn clone(&self) -> Self {
    Self {
      buf: Arc::clone(&self.buf),
      cur: self.cur,
      size: self.size,
    }
  }
}
