import gleam/list
import gleam/string
import problem7.{Ack, Close, Connect, Data}

pub fn parse_connect_test() {
  assert problem7.parse_message(<<"/connect/12345/":utf8>>)
    == Ok(Connect(12_345))
}

pub fn parse_connect_session_zero_test() {
  assert problem7.parse_message(<<"/connect/0/":utf8>>) == Ok(Connect(0))
}

pub fn parse_data_test() {
  assert problem7.parse_message(<<"/data/12345/0/hello/":utf8>>)
    == Ok(Data(12_345, 0, "hello"))
}

pub fn parse_data_empty_payload_test() {
  assert problem7.parse_message(<<"/data/12345/0//":utf8>>)
    == Ok(Data(12_345, 0, ""))
}

pub fn parse_data_with_escaped_slash_test() {
  assert problem7.parse_message(<<"/data/12345/0/foo\\/bar/":utf8>>)
    == Ok(Data(12_345, 0, "foo\\/bar"))
}

pub fn parse_data_with_escaped_backslash_test() {
  assert problem7.parse_message(<<"/data/12345/0/foo\\\\bar/":utf8>>)
    == Ok(Data(12_345, 0, "foo\\\\bar"))
}

pub fn parse_ack_test() {
  assert problem7.parse_message(<<"/ack/12345/100/":utf8>>)
    == Ok(Ack(12_345, 100))
}

pub fn parse_close_test() {
  assert problem7.parse_message(<<"/close/12345/":utf8>>) == Ok(Close(12_345))
}

pub fn parse_missing_leading_slash_test() {
  assert problem7.parse_message(<<"connect/12345/":utf8>>) == Error(Nil)
}

pub fn parse_missing_trailing_slash_test() {
  assert problem7.parse_message(<<"/connect/12345":utf8>>) == Error(Nil)
}

pub fn parse_unknown_type_test() {
  assert problem7.parse_message(<<"/unknown/12345/":utf8>>) == Error(Nil)
}

pub fn parse_negative_session_test() {
  assert problem7.parse_message(<<"/connect/-1/":utf8>>) == Error(Nil)
}

pub fn parse_numeric_overflow_test() {
  assert problem7.parse_message(<<"/connect/2147483648/":utf8>>) == Error(Nil)
}

pub fn parse_numeric_just_under_max_test() {
  assert problem7.parse_message(<<"/connect/2147483647/":utf8>>)
    == Ok(Connect(2_147_483_647))
}

pub fn parse_non_numeric_session_test() {
  assert problem7.parse_message(<<"/connect/abc/":utf8>>) == Error(Nil)
}

pub fn parse_empty_message_test() {
  assert problem7.parse_message(<<"":utf8>>) == Error(Nil)
}

pub fn parse_just_slashes_test() {
  assert problem7.parse_message(<<"//":utf8>>) == Error(Nil)
}

pub fn parse_connect_extra_fields_test() {
  assert problem7.parse_message(<<"/connect/123/extra/":utf8>>) == Error(Nil)
}

pub fn parse_invalid_utf8_test() {
  assert problem7.parse_message(<<0xFF, 0xFE>>) == Error(Nil)
}

pub fn serialize_connect_test() {
  assert problem7.serialize_message(Connect(12_345)) == "/connect/12345/"
}

pub fn serialize_data_test() {
  assert problem7.serialize_message(Data(12_345, 0, "hello"))
    == "/data/12345/0/hello/"
}

pub fn serialize_data_empty_test() {
  assert problem7.serialize_message(Data(12_345, 0, "")) == "/data/12345/0//"
}

pub fn serialize_ack_test() {
  assert problem7.serialize_message(Ack(12_345, 100)) == "/ack/12345/100/"
}

pub fn serialize_close_test() {
  assert problem7.serialize_message(Close(12_345)) == "/close/12345/"
}

pub fn escape_empty_test() {
  assert problem7.escape("") == ""
}

pub fn escape_no_special_chars_test() {
  assert problem7.escape("hello") == "hello"
}

pub fn escape_backslash_test() {
  assert problem7.escape("foo\\bar") == "foo\\\\bar"
}

pub fn escape_forward_slash_test() {
  assert problem7.escape("foo/bar") == "foo\\/bar"
}

pub fn escape_both_test() {
  assert problem7.escape("a\\b/c") == "a\\\\b\\/c"
}

pub fn escape_consecutive_slashes_test() {
  assert problem7.escape("//") == "\\/\\/"
}

pub fn unescape_empty_test() {
  assert problem7.unescape("") == ""
}

pub fn unescape_no_special_chars_test() {
  assert problem7.unescape("hello") == "hello"
}

pub fn unescape_forward_slash_test() {
  assert problem7.unescape("foo\\/bar") == "foo/bar"
}

pub fn unescape_backslash_test() {
  assert problem7.unescape("foo\\\\bar") == "foo\\bar"
}

pub fn unescape_both_test() {
  assert problem7.unescape("a\\\\b\\/c") == "a\\b/c"
}

pub fn unescape_escaped_backslash_before_slash_test() {
  assert problem7.unescape("\\\\/") == "\\/"
}

pub fn escape_unescape_roundtrip_test() {
  let original = "hello/world\\test"
  assert problem7.unescape(problem7.escape(original)) == original
}

pub fn escape_unescape_roundtrip_empty_test() {
  assert problem7.unescape(problem7.escape("")) == ""
}

pub fn escape_unescape_roundtrip_complex_test() {
  let original = "a/b\\c/d\\\\e//f"
  assert problem7.unescape(problem7.escape(original)) == original
}

pub fn process_lines_single_complete_line_test() {
  assert problem7.process_lines("", "hello\n") == #("olleh\n", "")
}

pub fn process_lines_multiple_lines_test() {
  assert problem7.process_lines("", "hello\nworld\n") == #("olleh\ndlrow\n", "")
}

pub fn process_lines_partial_line_test() {
  assert problem7.process_lines("", "hello") == #("", "hello")
}

pub fn process_lines_partial_then_complete_test() {
  assert problem7.process_lines("hel", "lo\n") == #("olleh\n", "")
}

pub fn process_lines_empty_line_test() {
  assert problem7.process_lines("", "\n") == #("\n", "")
}

pub fn process_lines_mixed_complete_and_partial_test() {
  assert problem7.process_lines("", "hello\nwor") == #("olleh\n", "wor")
}

pub fn process_lines_empty_input_test() {
  assert problem7.process_lines("buf", "") == #("", "buf")
}

pub fn process_lines_multiple_empty_lines_test() {
  assert problem7.process_lines("", "\n\n\n") == #("\n\n\n", "")
}

pub fn chunk_small_data_test() {
  let chunks = problem7.chunk_for_send(100, 0, "hello")
  assert chunks == ["/data/100/0/hello/"]
}

pub fn chunk_empty_data_test() {
  let chunks = problem7.chunk_for_send(100, 0, "")
  assert chunks == []
}

pub fn chunk_data_with_offset_test() {
  let chunks = problem7.chunk_for_send(100, 50, "hello")
  assert chunks == ["/data/100/50/hello/"]
}

pub fn chunk_data_needing_escape_test() {
  let chunks = problem7.chunk_for_send(100, 0, "a/b")
  assert chunks == ["/data/100/0/a\\/b/"]
}

pub fn chunk_large_data_produces_multiple_test() {
  let data = string.repeat("a", 600)
  let chunks = problem7.chunk_for_send(100, 0, data)
  assert list.length(chunks) > 1
  list.each(chunks, fn(chunk) {
    assert string.byte_size(chunk) < 1000
  })
}

pub fn chunk_positions_are_sequential_test() {
  let data = string.repeat("x", 600)
  let chunks = problem7.chunk_for_send(42, 10, data)
  assert list.first(chunks)
    == Ok("/data/42/10/" <> string.repeat("x", 480) <> "/")
  let assert [_, second, ..] = chunks
  assert string.starts_with(second, "/data/42/490/")
}
