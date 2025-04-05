use std::{collections::HashMap, sync::Mutex};

use lazy_static::lazy_static;

lazy_static! {
  pub static ref DIRECT_BUF_MANAGER: Mutex<DirectBufferManager> =
    Mutex::new(DirectBufferManager::new());
}

pub struct DirectBufferManager {
  direct_buf_ref: HashMap<String, Box<[u8]>>,
}

impl DirectBufferManager {
  pub fn new() -> DirectBufferManager {
    DirectBufferManager {
      direct_buf_ref: HashMap::new(),
    }
  }

  pub fn hold(&mut self, key: &str, value: Box<[u8]>) -> Result<(), &'static str> {
    if self.direct_buf_ref.get(key).is_some() {
      return Err("[bytebuffer::hold()] key already exists !!!");
    };
    self.direct_buf_ref.insert(String::from(key), value);
    Ok(())
  }

  pub fn release(&mut self, key: &str) -> Result<(), &'static str> {
    if let None = self.direct_buf_ref.remove(key) {
      return Err("[bytebuffer::release()] key doesn't exist !!!");
    };
    Ok(())
  }
}
