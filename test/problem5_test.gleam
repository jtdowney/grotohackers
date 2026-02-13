import problem5

pub fn is_boguscoin_address_valid_test() {
  assert problem5.is_boguscoin_address("7F1u3wSD5RbOHQmupo9nx4TnhQ")
  assert problem5.is_boguscoin_address("7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX")
  assert problem5.is_boguscoin_address("7LOrwbDlS8NujgjddyogWgIM93MV5N2VR")
  assert problem5.is_boguscoin_address("7adNeSwJkMakpEcln9HEtthSRtxdmEHOT8T")
}

pub fn is_boguscoin_address_tonys_address_test() {
  assert problem5.is_boguscoin_address("7YWHMfk9JZe0LM0g1ZauHuiSxhI")
}

pub fn is_boguscoin_address_too_short_test() {
  assert !problem5.is_boguscoin_address("7aaaaaaaaaaaaaaaaaaaaaaaa")
  assert !problem5.is_boguscoin_address("7")
}

pub fn is_boguscoin_address_too_long_test() {
  assert !problem5.is_boguscoin_address("7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaA")
}

pub fn is_boguscoin_address_wrong_prefix_test() {
  assert !problem5.is_boguscoin_address("8F1u3wSD5RbOHQmupo9nx4TnhQ")
  assert !problem5.is_boguscoin_address("xF1u3wSD5RbOHQmupo9nx4TnhQ")
}

pub fn is_boguscoin_address_non_alphanumeric_test() {
  assert !problem5.is_boguscoin_address("7F1u3wSD5RbOHQmupo9nx4Tnh!")
  assert !problem5.is_boguscoin_address("7F1u3wSD5RbOHQmupo9nx4Tnh-")
  assert !problem5.is_boguscoin_address("7F1u3wSD5Rb OHQmupo9nx4TnhQ")
}

pub fn is_boguscoin_address_empty_test() {
  assert !problem5.is_boguscoin_address("")
}

pub fn rewrite_boguscoin_no_address_test() {
  assert problem5.rewrite_boguscoin("Hello, world!") == "Hello, world!"
}

pub fn rewrite_boguscoin_single_address_test() {
  assert problem5.rewrite_boguscoin(
      "Please pay 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX ok?",
    )
    == "Please pay 7YWHMfk9JZe0LM0g1ZauHuiSxhI ok?"
}

pub fn rewrite_boguscoin_at_start_test() {
  assert problem5.rewrite_boguscoin(
      "7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX is my address",
    )
    == "7YWHMfk9JZe0LM0g1ZauHuiSxhI is my address"
}

pub fn rewrite_boguscoin_at_end_test() {
  assert problem5.rewrite_boguscoin("Send to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX")
    == "Send to 7YWHMfk9JZe0LM0g1ZauHuiSxhI"
}

pub fn rewrite_boguscoin_only_address_test() {
  assert problem5.rewrite_boguscoin("7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX")
    == "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
}

pub fn rewrite_boguscoin_multiple_addresses_test() {
  assert problem5.rewrite_boguscoin(
      "7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX and 7LOrwbDlS8NujgjddyogWgIM93MV5N2VR",
    )
    == "7YWHMfk9JZe0LM0g1ZauHuiSxhI and 7YWHMfk9JZe0LM0g1ZauHuiSxhI"
}

pub fn rewrite_boguscoin_preserves_non_address_7_words_test() {
  assert problem5.rewrite_boguscoin("7up is a drink") == "7up is a drink"
}

pub fn process_buffer_empty_test() {
  let #(lines, remainder) = problem5.process_buffer("", "")
  assert lines == []
  assert remainder == ""
}

pub fn process_buffer_single_line_test() {
  let #(lines, remainder) = problem5.process_buffer("", "hello\n")
  assert lines == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_partial_test() {
  let #(lines, remainder) = problem5.process_buffer("", "hello")
  assert lines == []
  assert remainder == "hello"
}

pub fn process_buffer_multi_line_test() {
  let #(lines, remainder) = problem5.process_buffer("", "line1\nline2\n")
  assert lines == ["line1", "line2"]
  assert remainder == ""
}

pub fn process_buffer_crlf_stripping_test() {
  let #(lines, remainder) = problem5.process_buffer("", "hello\r\n")
  assert lines == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_partial_continuation_test() {
  let #(lines1, buffer) = problem5.process_buffer("", "hel")
  assert lines1 == []

  let #(lines2, remainder) = problem5.process_buffer(buffer, "lo\n")
  assert lines2 == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_mixed_complete_and_partial_test() {
  let #(lines, remainder) = problem5.process_buffer("", "line1\npartial")
  assert lines == ["line1"]
  assert remainder == "partial"
}
