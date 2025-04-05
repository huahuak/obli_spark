pub fn sprint_byte(byt: &[u8]) -> String {
  let mut s = String::new();
  let mut cnt = 0;
  unsafe {
    s.push_str(&format!(
      "// ------------------ print byte ------------------ //\n"
    ));
    for i in byt {
      s.push_str(&format!("{:02x} ", i));
      cnt += 1;
      if cnt == 16 {
        s.push('\n');
        cnt = 0;
      }
    }
    s.push('\n');
    for i in byt {
      s.push_str(&format!("{}", char::from(*i)));
    }
    s.push('\n');
    s.push_str(&format!("// ------------------ print byte ------------------ //\n"));
  }
  s
}
