use std::fmt::format;

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use super::vector_generated::org::kaihua::obliop::collection::fbs::{FieldUnion, RowTable};

pub struct FbsRowTable<'fbb> {
  fbb: FlatBufferBuilder<'fbb>,
  // rows: Vec<WIPOffset<Field<'fbb>>>,
}

impl<'fbb> FbsRowTable<'fbb> {
  pub fn new() -> FbsRowTable<'fbb> {
    FbsRowTable {
      fbb: FlatBufferBuilder::with_capacity(1024),
    }
  }

  pub fn create_fields_cell() {}
}

pub fn sprint_row_table_fbs(fbs: &[u8]) -> String {
  let mut s = String::new();
  let rb = flatbuffers::root::<RowTable>(fbs).unwrap();
  for r in rb.rows().unwrap() {
    for f in r.fields().unwrap() {
      match f.value_type() {
        FieldUnion::IntValue => {
          s.push_str(format!("{} | ", f.value_as_int_value().unwrap().value()).as_str());
        }
        FieldUnion::StringValue => {
          s.push_str(
            format!("{} | ", f.value_as_string_value().unwrap().value().unwrap()).as_str(),
          );
        }
        FieldUnion::DoubleValue => {
          s.push_str(format!("{} | ", f.value_as_double_value().unwrap().value()).as_str());
        }
        _ => {
          panic!("[fbs_vec::print_row_table_fbs] value type unsupported !!!");
        }
      }
    }
    s.push_str("\n");
  }
  s
}
