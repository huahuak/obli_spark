use std::{
  collections::hash_map::DefaultHasher,
  hash::{Hash, Hasher},
};

use flatbuffers::WIPOffset;
use proto::{
  collection::vector_generated::org::kaihua::obliop::collection::fbs::{
    Field, FieldArgs, FieldUnion, IntValue, IntValueArgs, Row, RowArgs, RowTable, RowTableArgs,
  },
  protocol::context::ObliData,
};

use crate::data::manager::DATA_MANAGER;

pub fn hash_exec(input: &ObliData, output: &ObliData) -> Result<(), &'static str> {
  let mut hasher = DefaultHasher::new();

  let dm = DATA_MANAGER.exclusive_access();
  let buf = dm.get_data(&input.id).unwrap().buffer.lock().unwrap();

  // fbs
  let mut fbb = flatbuffers::FlatBufferBuilder::with_capacity(1024);
  let mut field_vec: Vec<WIPOffset<Field>> = vec![];
  let mut row_vec: Vec<WIPOffset<Row>> = vec![];
  let r_table = flatbuffers::root::<RowTable>(&buf).unwrap();
  for row in r_table.rows().unwrap() {
    for field in row.fields().unwrap() {
      let hash_result;
      match field.value_type() {
        FieldUnion::IntValue => {
          let i = field.value_as_int_value().unwrap().value();
          i.hash(&mut hasher);
          hash_result = hasher.finish();
        }
        FieldUnion::StringValue => {
          let s = field.value_as_string_value().unwrap().value();
          s.hash(&mut hasher);
          hash_result = hasher.finish();
        }
        // FieldUnion::DoubleValue => {}
        _ => {
          return Err("[operator::hasher::hash_exec()] unsupported type can't process");
          // panic!()
        }
      }

      let int_value = IntValue::create(
        &mut fbb,
        &IntValueArgs {
          value: hash_result as i32,
        },
      );
      let field_value = Field::create(
        &mut fbb,
        &FieldArgs {
          value_type: FieldUnion::IntValue,
          value: Some(int_value.as_union_value()),
          is_null: false,
        },
      );
      field_vec.push(field_value);
    }
    let field_vec = fbb.create_vector(&field_vec);
    let row_value = Row::create(
      &mut fbb,
      &RowArgs {
        fields: Some(field_vec),
      },
    );
    row_vec.push(row_value);
  }
  let row_vec = fbb.create_vector(&row_vec);
  let row_table = RowTable::create(
    &mut fbb,
    &RowTableArgs {
      rows: Some(row_vec),
    },
  );
  fbb.finish(row_table, None);
  let buf_result = fbb.finished_data();

  // release immutable shared ref `dm`
  drop(buf);
  drop(dm);

  let mut dm = DATA_MANAGER.exclusive_access();
  // push fbs buf_result to output data
  if let Some(data) = dm.get_data_mut(&output.id) {
    buf_result
      .iter()
      .for_each(|item| data.buffer.lock().unwrap().push(*item));
  } else {
    // panic!("[operator::hasher::hash_exec()] output data don't exist");
    return Err("[operator::hasher::hash_exec()] output data don't exist");
  }
  drop(dm);
  Ok(())
}
