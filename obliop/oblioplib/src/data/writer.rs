use flatbuffers::{FlatBufferBuilder, WIPOffset};
use proto::{collection::vector_generated::org::kaihua::obliop::collection::fbs::*, config};

use super::manager::{Item, Record};

pub struct DataWriter<'a> {
  fbb: FlatBufferBuilder<'a>,
  row_vec: Vec<WIPOffset<Row<'a>>>,
}

impl<'a> DataWriter<'a> {
  pub fn new() -> DataWriter<'a> {
    DataWriter {
      fbb: flatbuffers::FlatBufferBuilder::with_capacity(config::BLOCK_SIZE),
      row_vec: vec![],
    }
  }

  pub fn write(&mut self, record: &Record) {
    let fbb = &mut self.fbb;
    let mut field_vec = vec![];
    for ele in &record.items {
      match ele {
        Item::Int(i) => {
          let int_value = IntValue::create(fbb, &IntValueArgs { value: *i });
          let field = Field::create(
            fbb,
            &FieldArgs {
              value_type: FieldUnion::IntValue,
              value: Some(int_value.as_union_value()),
              is_null: false,
            },
          );
          field_vec.push(field);
        }
        Item::Str(s) => {
          let s = fbb.create_string(&s);
          let str = StringValue::create(fbb, &StringValueArgs { value: Some(s) });
          let field = Field::create(
            fbb,
            &FieldArgs {
              value_type: FieldUnion::StringValue,
              value: Some(str.as_union_value()),
              is_null: false,
            },
          );
          field_vec.push(field);
        }
        Item::Double(d) => {
          let double_value = DoubleValue::create(fbb, &DoubleValueArgs { value: *d });
          let field = Field::create(
            fbb,
            &FieldArgs {
              value_type: FieldUnion::DoubleValue,
              value: Some(double_value.as_union_value()),
              is_null: false,
            },
          );
          field_vec.push(field);
        }
        // @todo return error if not match
        _ => {}
      }
    }
    let field_vec = fbb.create_vector(&field_vec);
    let row_value = Row::create(
      fbb,
      &RowArgs {
        fields: Some(field_vec),
      },
    );
    self.row_vec.push(row_value);
  }

  pub fn finish(&mut self) -> &[u8] {
    let row_vec = self.fbb.create_vector(&self.row_vec);
    let row_table = RowTable::create(
      &mut self.fbb,
      &RowTableArgs {
        rows: Some(row_vec),
      },
    );
    self.fbb.finish(row_table, None);
    self.fbb.finished_data()
  }
}
