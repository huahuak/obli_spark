use std::{
  any::Any,
  collections::{HashMap, VecDeque},
};

use lazy_static::lazy_static;
use proto::{protocol::context::ObliData, sync::UPSafeCell};

use crate::data::{manager::Record, reader::DataIterator};

lazy_static! {
  pub static ref CTX_MANAGER: UPSafeCell<CtxManager> =
    unsafe { UPSafeCell::new(CtxManager::new()) };
}

pub struct CtxManager {
  ctxs: HashMap<String, Ctx>,
}

impl CtxManager {
  fn new() -> CtxManager {
    CtxManager {
      ctxs: HashMap::new(),
    }
  }

  pub fn find(&mut self, id: &String) -> Option<&mut Ctx> {
    self.ctxs.get_mut(id)
  }

  pub fn insert(&mut self, id: &String, member: Box<dyn ContinuousCompute>) {
    self.ctxs.insert(
      id.to_string(),
      Ctx {
        expr_id: id.to_string(),
        member,
        stash_values: HashMap::new(),
      },
    );
  }
}

pub struct Ctx {
  pub expr_id: String,
  pub member: Box<dyn ContinuousCompute>,
  pub stash_values: HashMap<&'static str, Box<dyn Any>>,
}

impl Ctx {
  fn new(expr_id: String, member: Box<dyn ContinuousCompute>) -> Ctx {
    Ctx {
      expr_id,
      member,
      stash_values: HashMap::new(),
    }
  }

  pub fn contine_compute(&mut self) -> Result<(), &'static str> {
    self.member.as_mut().compute()?;
    Ok(())
  }
}

pub trait ContinuousCompute {
  fn compute(&mut self) -> Result<bool, &'static str>;
  fn as_any(&mut self) -> &mut dyn Any;
}

pub struct ContinuousIterator {
  cur: Option<DataIterator>,
  blocks: VecDeque<ObliData>,
}

impl ContinuousIterator {
  pub fn new() -> Self {
    Self {
      cur: None,
      blocks: VecDeque::new(),
    }
  }

  pub fn append(&mut self, data: ObliData) {
    self.blocks.push_back(data);
  }
}

impl Iterator for ContinuousIterator {
  type Item = Record;

  fn next(&mut self) -> Option<Self::Item> {
    if self.cur.is_none() && self.blocks.is_empty() {
      return None;
    }
    if self.cur.is_none() {
      self.cur = Some(DataIterator::new(self.blocks.pop_front().as_ref().unwrap()));
    }
    let mut ret = self.cur.as_mut().unwrap().next();
    if ret.is_none() && !self.blocks.is_empty() {
      // swtich to new iterator
      self.cur = Some(DataIterator::new(&self.blocks.pop_front().unwrap()));
      ret = self.cur.as_mut().unwrap().next();
    }
    ret
  }
}
