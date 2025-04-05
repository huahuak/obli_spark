// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use serde::{Deserialize, Serialize};

pub mod collection;
// pub mod context;
pub mod protocol;
pub mod sync;
pub mod util;
pub mod config;


#[derive(Deserialize, Serialize, Debug)]
pub enum Command {
  OpCtxExec,
  DataSend,
  DataGet,
  Unknown,
}

impl From<u32> for Command {
  #[inline]
  fn from(value: u32) -> Command {
    match value {
      0 => Command::OpCtxExec,
      1 => Command::DataSend,
      2 => Command::DataGet,
      _ => Command::Unknown,
    }
  }
}

impl From<Command> for u32 {
  #[inline]
  fn from(value: Command) -> u32 {
    match value {
      Command::OpCtxExec => 0,
      Command::DataSend => 1,
      Command::DataGet => 2,
      Command::Unknown => 99,
    }
  }
}

pub const UUID: &str = &include_str!(concat!(env!("OUT_DIR"), "/uuid.txt"));
