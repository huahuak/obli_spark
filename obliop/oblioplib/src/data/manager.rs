use std::{
  cmp::Ordering,
  collections::HashMap,
  path,
  sync::{Arc, Mutex},
  vec,
};

use lazy_static::lazy_static;
use proto::protocol::context::{ObliData, TaCallerInfo};
use proto::sync::UPSafeCell;

use crate::logger::LOGGER;

#[derive(Debug, PartialEq, Clone)]
pub enum Item {
  Int(i32),
  Str(String),
  Double(f64),
}

impl PartialOrd for Item {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    match (self, other) {
      (Item::Int(a), Item::Int(b)) => a.partial_cmp(b),
      (Item::Str(a), Item::Str(b)) => a.partial_cmp(b),
      (Item::Double(a), Item::Double(b)) => a.partial_cmp(b),
      _ => {
        None
      }
    }
  }
}

#[derive(Debug)]
pub struct Record {
  pub items: Vec<Item>,
}

impl Record {
  pub fn union(&self, other: &Record) -> Record {
    let mut ret = Record { items: vec![] };
    for ele in &self.items {
      ret.items.push(ele.clone());
    }
    for ele in &other.items {
      ret.items.push(ele.clone());
    }
    ret
  }

  pub fn gen_dummy_with_same_schema(&self) -> Record {
    let mut ret = Record { items: vec![] };
    for ele in &self.items {
      ret.items.push(match ele {
        Item::Int(_) => Item::Int(0),
        Item::Str(_) => Item::Str("null".to_string()),
        Item::Double(_) => Item::Double(0.0),
      });
    }
    ret
  }
}

#[derive(Debug)]
pub struct Data {
  pub description: Arc<Mutex<ObliData>>,
  pub buffer: Arc<Mutex<Vec<u8>>>,
}

pub struct DataManager {
  map: HashMap<String, Data>,
}

impl DataManager {
  pub fn new() -> DataManager {
    DataManager {
      map: HashMap::new(),
    }
  }

  /// insert will clone data struct
  pub fn insert(
    &mut self,
    key: &str,
    data: &ObliData,
    buffer: &[u8],
    is_prepared: bool,
  ) -> Result<(), &'static str> {
    // assert!(
    //   self.get_data_mut(key).is_none(),
    //   "[data::manager::inser()] DATA_MANAGER already had the data with key '{}'",
    //   key
    // );

    // prevent the data of same key
    if !self.get_data_mut(key).is_none() {
      return Err("[data::manager::inser()] DATA_MANAGER already had the data");
    }

    let key = String::from(key);
    let mut data = data.clone();
    data.prepared = is_prepared;
    self.map.insert(
      key,
      Data {
        description: Arc::new(Mutex::new(data)),
        buffer: Arc::new(Mutex::new(buffer.to_vec())),
      },
    );
    Ok(())
  }

  pub fn get_data_mut(&mut self, key: &str) -> Option<&mut Data> {
    self.map.get_mut(key)
  }

  pub fn get_data(&self, key: &str) -> Option<&Data> {
    self.map.get(key)
  }
}

lazy_static! {
  pub static ref DATA_MANAGER: UPSafeCell<DataManager> =
    unsafe { UPSafeCell::new(DataManager::new()) };
}

pub fn push_data_handler(data: &ObliData, buffer: &[u8]) -> Result<(), &'static str> {
  // trace_println!("[data::mod.rs] enter fn data_handler");
  DATA_MANAGER
    .exclusive_access()
    .insert(&data.id, data, buffer, true)?;
  Ok(())
}

pub fn get_data_handler(
  target: &ObliData,
  info: &mut [u8],
  output: &mut [u8],
) -> Result<(), &'static str> {
  let dm = DATA_MANAGER.exclusive_access();
  // let buf = &dm.get_data(&target.id).unwrap().buffer;
  let mut v = TaCallerInfo {
    method: proto::Command::DataGet,
    info: String::new(),
    ok: false,
  };

  if let Some(data) = dm.get_data(&target.id) {
    let buf = &data.buffer;
    let buf_len = buf.lock().unwrap().len();
    output[..buf_len].copy_from_slice(&buf.lock().unwrap());
    v.info.insert_str(0, "data ready".to_string().as_ref());
    v.ok = true;
  } else {
    LOGGER.lock().unwrap().log(String::from(
      "[ta::manager.rs::get_data_handle()] data not ready",
    ));
    v.info.insert_str(0, "data not ready".to_string().as_ref());
    v.ok = false;
  }

  let info_json = serde_json::to_string(&v).unwrap();
  let info_json_byte = info_json.as_bytes();
  info[..info_json_byte.len()].copy_from_slice(info_json_byte);
  drop(dm);
  Ok(())
}
