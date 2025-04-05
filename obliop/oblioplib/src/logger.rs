use std::sync::Mutex;

use lazy_static::lazy_static;

pub struct Logger {
  log_buf: Vec<String>,
  cur: usize,
}

lazy_static! {
  pub static ref LOGGER: Mutex<Logger> = Mutex::new(Logger::new());
}

impl Logger {
  fn new() -> Logger {
    Logger {
      log_buf: vec![],
      cur: 0,
    }
  }

  pub fn log(&mut self, info: String) {
    self.log_buf.push(info);
  }

  pub fn next(&mut self) -> Option<&String> {
    if self.log_buf.len() > self.cur {
      let ret = self.log_buf.get(self.cur);
      self.cur += 1;
      ret
    } else {
      self.log_buf.clear();
      None
    }
  }
}
