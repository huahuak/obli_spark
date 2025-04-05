use std::char;

#[allow(dead_code)]
pub fn print_byte(byt: &[u8]) {
  println!("// ------------------ print byte ------------------ //");
  for i in byt {
    print!("{:02x} ", i);
  }
  println!();
  for i in byt {
    print!("{}", char::from(*i));
  }
  println!();
  println!("// ------------------ print byte ------------------ //");
}
