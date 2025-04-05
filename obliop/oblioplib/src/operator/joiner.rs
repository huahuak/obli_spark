use core::str;
use std::{
  any::Any,
  borrow::BorrowMut,
  cmp::Ordering::{Equal, Greater, Less},
  vec,
};

use proto::{
  config,
  protocol::context::{JoinKeyInfo, ObliData},
};

use crate::data::{
  manager::{Record, DATA_MANAGER},
  reader::DataIterator,
  writer::DataWriter,
};

use super::ctx::ContinuousCompute;

/**
 * @author kahua.li
 * @email moflowerlkh@gmail.com
 * @date 2023/03/13
 **/

pub struct JoinManager {
  output_size: usize,  // every output block size
  buffer: Vec<Record>, // use to cache output
  join_key: Vec<JoinKeyInfo>,
  // left input
  l_cur_record: Option<Record>,
  l_records: Vec<Record>,
  // right input
  r_begin: usize,
  r_current: usize,
  r_records: Vec<Record>,
  // output
  output_block: Option<ObliData>,
}

impl JoinManager {
  pub fn new(join_key: Vec<JoinKeyInfo>) -> Self {
    Self {
      output_size: config::OUTPUT_SIZE, // @todo parameterization
      buffer: vec![],
      join_key,
      l_cur_record: None,
      l_records: vec![],
      r_begin: 0,
      r_current: 0,
      r_records: vec![],
      output_block: None,
    }
  }

  pub fn registry_output_block(&mut self, output: ObliData) -> Result<(), &'static str> {
    self.output_block = Some(output);
    Ok(())
  }

  pub fn registry_left_block(&mut self, left: ObliData) -> Result<(), &'static str> {
    let mut new_record = DataIterator::new(&left).borrow_mut().into();
    self.l_records.append(&mut new_record);
    Ok(())
  }

  pub fn registry_right_block(&mut self, right: ObliData) -> Result<(), &'static str> {
    // first, remove old record.
    self.r_records.drain(0..self.r_begin);
    self.r_current -= self.r_begin;
    self.r_begin = 0;
    // second, append new record.
    let mut new_record: Vec<Record> = DataIterator::new(&right).borrow_mut().into();
    self.r_records.append(&mut new_record);
    Ok(())
  }

  fn output(&mut self, result: Vec<Record>) -> Result<(), &'static str> {
    if result.len() == 0 {
      return Ok(());
    }
    // while result.len() > self.output_size {
    //   self.buffer.push(result.pop().unwrap())
    // }
    // while result.len() < self.output_size && self.buffer.len() > 0 {
    //   result.push(self.buffer.pop().unwrap());
    // }
    // while result.len() < self.output_size {
    //   result.push(result.get(0).unwrap().gen_dummy_with_same_schema());
    // }

    let mut writer = DataWriter::new();
    result.iter().for_each(|record| writer.write(record));
    let byt_buf = writer.finish();

    let mut dm = DATA_MANAGER.exclusive_access();
    // push fbs buf_result to output data
    if let Some(data) = dm.get_data_mut(&self.output_block.as_ref().unwrap().id) {
      byt_buf
        .iter()
        .for_each(|item| data.buffer.lock().unwrap().push(*item));
    } else {
      return Err("[operator::hasher::sort_exec()] output data don't exist");
    }
    drop(dm);

    Ok(())
  }
}

impl ContinuousCompute for JoinManager {
  /// return value:
  /// true is need continue compute,
  /// false is finish compute.
  fn compute(&mut self) -> Result<bool, &'static str> {
    let mut result = vec![];
    let mut cnt = 0;
    loop {
      cnt += 1;
      // current left record is none, move to next record.
      // if the current is some, then continue last computation.
      if self.l_cur_record.is_none() {
        self.l_cur_record = if self.l_records.is_empty() {
          None
        } else {
          Some(self.l_records.remove(0))
        };
        self.r_current = self.r_begin;
      }
      // the right or left records is none.
      if self.r_current >= self.r_records.len() || self.l_cur_record.is_none() {
        break;
      }
      // @todo now only support one join key
      let l_record = self.l_cur_record.as_ref().unwrap();
      let r_record = &self.r_records[self.r_current];
      let l = &l_record.items[self.join_key.first().unwrap().lpos as usize];
      let r = &r_record.items[self.join_key.first().unwrap().rpos as usize];
      match l.partial_cmp(&r) {
        Some(cmp) => match cmp {
          Equal => {
            result.push(l_record.union(&r_record));
            self.r_current += 1;
          }
          Greater => {
            // the left is greater, just move the right to the next one;
            self.r_begin += 1;
            self.r_current = self.r_begin;
          }
          Less => {
            // resume r_current, set l_cur_record None then loop will move to the next one.
            self.l_cur_record = None;
          }
        },
        None => return Err("TA::joiner::compute() l and r record partial_cmp panic!!!"),
      }
    }
    println!("do {} loop", cnt);
    self.output(result)?;
    Ok(true)
  }

  fn as_any(&mut self) -> &mut dyn Any {
    self
  }
}
