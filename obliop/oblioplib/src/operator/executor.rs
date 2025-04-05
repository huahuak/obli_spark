use std::{sync::Arc, vec};

use proto::protocol::context::{
  Context,
  ExprType::{DATA, EQUIJOIN, HASH, MOD, SORT},
  Expression, ExtraExprInfo, JoinKeyInfo, SortOrderInfo,
};

use crate::data::manager::DATA_MANAGER;

use super::{
  ctx::{ContinuousCompute, CTX_MANAGER},
  hasher::hash_exec,
  joiner::JoinManager,
  sorter::sort_exec,
};

/**
 * @author kahua.li
 * @email moflowerlkh@gmail.com
 * @date 2023/02/04
 **/

pub fn execute(expr: &Expression) -> Result<(), &'static str> {
  for child in &expr.children {
    execute(&child)?;
  }
  let mut dm = DATA_MANAGER.exclusive_access();
  let mut inputs = vec![];
  for ele in &expr.children {
    inputs.push(Arc::clone(
      &dm
        .get_data(&ele.output.as_ref().borrow().id)
        .unwrap()
        .description,
    ));
  }
  let output = Arc::clone(
    &dm
      .get_data_mut(&expr.output.as_ref().borrow().id)
      .unwrap()
      .description,
  );
  drop(dm);

  for input in &inputs {
    if !input.lock().unwrap().prepared {
      return Err("[hasher.rs::hash_exec()] input isn't prepared !!!");
    }
    input.lock().unwrap().decrease_in_use()?;
  }

  // the expr has been executed by other tree
  if output.lock().unwrap().prepared {
    return Ok(());
  }

  match expr.typ {
    // MOD => {}
    HASH => {
      hash_exec(
        &inputs.get(0).unwrap().lock().unwrap(),
        &output.lock().unwrap(),
      )?;
    }
    SORT => {
      let sort_order_str = expr.info.get(&ExtraExprInfo::SortOrder).unwrap();
      let ordering: Vec<SortOrderInfo> = serde_json::from_str(&sort_order_str).unwrap();
      sort_exec(
        &inputs.get(0).unwrap().lock().unwrap(),
        &output.lock().unwrap(),
        ordering,
      )?;
    }
    EQUIJOIN => {
      // @audit need to remove Ctx in CTX_MANAGER!!!
      let mut cm = CTX_MANAGER.exclusive_access();
      if let Some(ctx) = cm.find(&expr.id) {
        // continuous computing in this branch
        let jm = ctx.member.as_any().downcast_mut::<JoinManager>().unwrap();
        jm.registry_left_block(inputs.get(0).unwrap().lock().unwrap().to_owned())?;
        jm.registry_right_block(inputs.get(1).unwrap().lock().unwrap().to_owned())?;
        jm.registry_output_block(output.lock().unwrap().to_owned())?;
        ctx.contine_compute()?;
      } else {
        // first, enter this branch
        let join_key_info_str = expr.info.get(&ExtraExprInfo::EquiJoinKey).unwrap();
        let join_key_info: Vec<JoinKeyInfo> = serde_json::from_str(&join_key_info_str).unwrap();
        let mut jm = JoinManager::new(join_key_info);
        jm.registry_left_block(inputs.get(0).unwrap().lock().unwrap().to_owned())?;
        jm.registry_right_block(inputs.get(1).unwrap().lock().unwrap().to_owned())?;
        jm.registry_output_block(output.lock().unwrap().to_owned())?;
        jm.compute()?;
        cm.insert(&expr.id, Box::new(jm));
      };
    }
    _ => {
      return Err("[executor.rs::execute()] expr typ of {:#?} is unsupported !!!");
    }
  }

  output.lock().unwrap().prepared = true;

  Ok(())
}

fn prepare_data(expr: &Expression) -> Result<(), &'static str> {
  for child in &expr.children {
    prepare_data(child)?;
  }
  let mut dm = DATA_MANAGER.exclusive_access();
  match expr.typ {
    DATA => {
      // data node output is input.
      let input = expr.output.as_ref().borrow();
      match dm.get_data_mut(&input.id) {
        Some(target) => target.description.lock().unwrap().increase_in_use(),
        None => return Err(
          "[TA::executor.rs::prepare_data()] can't find input data, which means need data from transport interface"),
      }
    }
    _ => {
      let output = expr.output.as_ref().borrow();
      if let None = dm.get_data(&output.id) {
        dm.insert(&output.id, &output, &vec![], false)?;
      }
    }
  }
  drop(dm);
  Ok(())
}

pub fn obli_op_ctx_exec(ctx: &mut Context) -> Result<(), &'static str> {
  for expr in &ctx.expressions {
    prepare_data(expr)?;
    execute(expr)?;
  }
  Ok(())
}
