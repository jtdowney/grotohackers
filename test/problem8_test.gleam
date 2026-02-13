import gleam/bit_array
import gleam/list
import problem8.{Add, AddPos, ReverseBits, SubPos, Xor, XorPos}

pub fn parse_cipher_spec_empty_test() {
  let assert Ok(#([], <<>>)) = problem8.parse_cipher_spec(<<0>>)
}

pub fn parse_cipher_spec_single_reversebits_test() {
  let assert Ok(#([ReverseBits], <<>>)) = problem8.parse_cipher_spec(<<1, 0>>)
}

pub fn parse_cipher_spec_single_xor_test() {
  let assert Ok(#([Xor(42)], <<>>)) = problem8.parse_cipher_spec(<<2, 42, 0>>)
}

pub fn parse_cipher_spec_single_xorpos_test() {
  let assert Ok(#([XorPos], <<>>)) = problem8.parse_cipher_spec(<<3, 0>>)
}

pub fn parse_cipher_spec_single_add_test() {
  let assert Ok(#([Add(10)], <<>>)) = problem8.parse_cipher_spec(<<4, 10, 0>>)
}

pub fn parse_cipher_spec_single_addpos_test() {
  let assert Ok(#([AddPos], <<>>)) = problem8.parse_cipher_spec(<<5, 0>>)
}

pub fn parse_cipher_spec_chained_test() {
  let assert Ok(#([Xor(1), ReverseBits], <<>>)) =
    problem8.parse_cipher_spec(<<2, 1, 1, 0>>)
}

pub fn parse_cipher_spec_invalid_opcode_test() {
  let assert Error(Nil) = problem8.parse_cipher_spec(<<99>>)
}

pub fn parse_cipher_spec_truncated_xor_test() {
  let assert Error(Nil) = problem8.parse_cipher_spec(<<2>>)
}

pub fn parse_cipher_spec_truncated_add_test() {
  let assert Error(Nil) = problem8.parse_cipher_spec(<<4>>)
}

pub fn parse_cipher_spec_no_terminator_test() {
  let assert Error(Nil) = problem8.parse_cipher_spec(<<1, 2, 42>>)
}

pub fn parse_cipher_spec_xor_zero_test() {
  let assert Ok(#([Xor(0)], <<>>)) = problem8.parse_cipher_spec(<<2, 0, 0>>)
}

pub fn parse_cipher_spec_add_zero_test() {
  let assert Ok(#([Add(0)], <<>>)) = problem8.parse_cipher_spec(<<4, 0, 0>>)
}

pub fn parse_cipher_spec_remaining_bytes_test() {
  let assert Ok(#([ReverseBits], <<42, 99>>)) =
    problem8.parse_cipher_spec(<<1, 0, 42, 99>>)
}

pub fn reverse_bits_zero_test() {
  assert problem8.reverse_bits(0x00) == 0x00
}

pub fn reverse_bits_ff_test() {
  assert problem8.reverse_bits(0xff) == 0xff
}

pub fn reverse_bits_01_test() {
  assert problem8.reverse_bits(0x01) == 0x80
}

pub fn reverse_bits_80_test() {
  assert problem8.reverse_bits(0x80) == 0x01
}

pub fn reverse_bits_a5_test() {
  assert problem8.reverse_bits(0xa5) == 0xa5
}

pub fn reverse_bits_0f_test() {
  assert problem8.reverse_bits(0x0f) == 0xf0
}

pub fn reverse_bits_roundtrip_test() {
  assert problem8.reverse_bits(problem8.reverse_bits(0x6b)) == 0x6b
}

pub fn apply_op_xor_test() {
  assert problem8.apply_op(Xor(0x0f), 0xff, 0) == 0xf0
}

pub fn apply_op_xorpos_test() {
  assert problem8.apply_op(XorPos, 0xff, 3) == 0xfc
}

pub fn apply_op_add_test() {
  assert problem8.apply_op(Add(10), 250, 0) == 4
}

pub fn apply_op_add_wraps_test() {
  assert problem8.apply_op(Add(1), 0xff, 0) == 0
}

pub fn apply_op_addpos_test() {
  assert problem8.apply_op(AddPos, 100, 5) == 105
}

pub fn apply_op_addpos_wraps_test() {
  assert problem8.apply_op(AddPos, 200, 100) == 44
}

pub fn apply_op_subpos_test() {
  assert problem8.apply_op(SubPos, 105, 5) == 100
}

pub fn apply_op_subpos_wraps_test() {
  assert problem8.apply_op(SubPos, 10, 20) == 246
}

pub fn apply_op_reversebits_test() {
  assert problem8.apply_op(ReverseBits, 0x01, 0) == 0x80
}

pub fn apply_cipher_xor_then_reverse_test() {
  assert problem8.apply_cipher([Xor(1), ReverseBits], 0x68, 0) == 0x96
}

pub fn apply_cipher_empty_test() {
  assert problem8.apply_cipher([], 0x42, 0) == 0x42
}

pub fn transform_bytes_problem_example_test() {
  let ops = [Xor(1), ReverseBits]
  let #(result, new_pos) = problem8.transform_bytes(ops, <<"hello":utf8>>, 0)
  assert result == <<0x96, 0x26, 0xb6, 0xb6, 0x76>>
  assert new_pos == 5
}

pub fn transform_bytes_empty_test() {
  let #(result, new_pos) = problem8.transform_bytes([Xor(1)], <<>>, 0)
  assert result == <<>>
  assert new_pos == 0
}

pub fn transform_bytes_position_advances_test() {
  let #(_, pos) = problem8.transform_bytes([AddPos], <<0, 0, 0>>, 10)
  assert pos == 13
}

pub fn invert_cipher_roundtrip_test() {
  let ops = [Xor(1), ReverseBits]
  let inverse = problem8.invert_cipher(ops)
  let #(encrypted, _) = problem8.transform_bytes(ops, <<"hello":utf8>>, 0)
  let #(decrypted, _) = problem8.transform_bytes(inverse, encrypted, 0)
  assert decrypted == <<"hello":utf8>>
}

pub fn invert_cipher_add_roundtrip_test() {
  let ops = [Add(100), XorPos, ReverseBits]
  let inverse = problem8.invert_cipher(ops)
  let data = <<"test data 123":utf8>>
  let #(encrypted, _) = problem8.transform_bytes(ops, data, 0)
  let #(decrypted, _) = problem8.transform_bytes(inverse, encrypted, 0)
  assert decrypted == data
}

pub fn invert_cipher_addpos_roundtrip_test() {
  let ops = [AddPos, Xor(0xaa)]
  let inverse = problem8.invert_cipher(ops)
  let data = <<"roundtrip":utf8>>
  let #(encrypted, _) = problem8.transform_bytes(ops, data, 5)
  let #(decrypted, _) = problem8.transform_bytes(inverse, encrypted, 5)
  assert decrypted == data
}

pub fn invert_op_reversebits_test() {
  assert problem8.invert_op(ReverseBits) == ReverseBits
}

pub fn invert_op_xor_test() {
  assert problem8.invert_op(Xor(42)) == Xor(42)
}

pub fn invert_op_xorpos_test() {
  assert problem8.invert_op(XorPos) == XorPos
}

pub fn invert_op_add_test() {
  assert problem8.invert_op(Add(10)) == Add(246)
}

pub fn invert_op_add_zero_test() {
  assert problem8.invert_op(Add(0)) == Add(0)
}

pub fn invert_op_addpos_test() {
  assert problem8.invert_op(AddPos) == SubPos
}

pub fn invert_op_subpos_test() {
  assert problem8.invert_op(SubPos) == AddPos
}

pub fn is_noop_empty_test() {
  assert problem8.is_noop([])
}

pub fn is_noop_xor_zero_test() {
  assert problem8.is_noop([Xor(0)])
}

pub fn is_noop_add_zero_test() {
  assert problem8.is_noop([Add(0)])
}

pub fn is_noop_double_reversebits_test() {
  assert problem8.is_noop([ReverseBits, ReverseBits])
}

pub fn is_noop_double_xor_test() {
  assert problem8.is_noop([Xor(0x42), Xor(0x42)])
}

pub fn is_noop_single_reversebits_not_noop_test() {
  assert !problem8.is_noop([ReverseBits])
}

pub fn is_noop_single_xor_not_noop_test() {
  assert !problem8.is_noop([Xor(1)])
}

pub fn is_noop_xorpos_not_noop_test() {
  assert !problem8.is_noop([XorPos])
}

pub fn is_noop_addpos_not_noop_test() {
  assert !problem8.is_noop([AddPos])
}

pub fn is_noop_add_nonzero_not_noop_test() {
  assert !problem8.is_noop([Add(1)])
}

pub fn parse_toys_basic_test() {
  let toys = problem8.parse_toys("10x toy car,15x dog,4x mass of slime")
  assert toys == [#(10, "toy car"), #(15, "dog"), #(4, "mass of slime")]
}

pub fn parse_toys_single_test() {
  let toys = problem8.parse_toys("3x teddy bear")
  assert toys == [#(3, "teddy bear")]
}

pub fn parse_toys_empty_test() {
  let toys = problem8.parse_toys("")
  assert toys == []
}

pub fn find_max_toy_basic_test() {
  let toys = [#(10, "toy car"), #(15, "dog"), #(4, "mass of slime")]
  let assert Ok("15x dog") = problem8.find_max_toy(toys)
}

pub fn find_max_toy_single_test() {
  let assert Ok("3x teddy bear") = problem8.find_max_toy([#(3, "teddy bear")])
}

pub fn find_max_toy_tie_takes_first_test() {
  let toys = [#(5, "alpha"), #(5, "beta")]
  let assert Ok("5x alpha") = problem8.find_max_toy(toys)
}

pub fn find_max_toy_empty_test() {
  let assert Error(Nil) = problem8.find_max_toy([])
}

pub fn handle_request_basic_test() {
  assert problem8.handle_request("10x toy car,15x dog,4x mass of slime")
    == "15x dog"
}

pub fn handle_request_single_toy_test() {
  assert problem8.handle_request("3x teddy bear") == "3x teddy bear"
}

pub fn process_line_buffer_empty_test() {
  let #(lines, remainder) = problem8.process_line_buffer("", "")
  assert lines == []
  assert remainder == ""
}

pub fn process_line_buffer_single_line_test() {
  let #(lines, remainder) = problem8.process_line_buffer("", "hello\n")
  assert lines == ["hello"]
  assert remainder == ""
}

pub fn process_line_buffer_partial_test() {
  let #(lines, remainder) = problem8.process_line_buffer("", "hello")
  assert lines == []
  assert remainder == "hello"
}

pub fn process_line_buffer_multi_line_test() {
  let #(lines, remainder) = problem8.process_line_buffer("", "line1\nline2\n")
  assert lines == ["line1", "line2"]
  assert remainder == ""
}

pub fn process_line_buffer_continuation_test() {
  let #(lines1, buffer) = problem8.process_line_buffer("", "hel")
  assert lines1 == []

  let #(lines2, remainder) = problem8.process_line_buffer(buffer, "lo\n")
  assert lines2 == ["hello"]
  assert remainder == ""
}

pub fn process_line_buffer_mixed_test() {
  let #(lines, remainder) = problem8.process_line_buffer("", "line1\npartial")
  assert lines == ["line1"]
  assert remainder == "partial"
}

pub fn full_roundtrip_all_ops_test() {
  let ops = [ReverseBits, Xor(0xab), XorPos, Add(42), AddPos]
  let inverse = problem8.invert_cipher(ops)
  let data = <<"10x toy car,15x dog\n":utf8>>
  let #(encrypted, end_pos) = problem8.transform_bytes(ops, data, 0)
  let #(decrypted, _) = problem8.transform_bytes(inverse, encrypted, 0)
  assert decrypted == data
  assert end_pos == 20
}

pub fn transform_bytes_position_dependent_test() {
  let ops = [XorPos]
  let #(result, _) = problem8.transform_bytes(ops, <<0, 0, 0, 0>>, 0)
  assert result == <<0, 1, 2, 3>>
}

pub fn is_noop_complex_identity_test() {
  assert problem8.is_noop([Add(128), Add(128)])
}

pub fn invert_cipher_order_test() {
  let ops = [Add(5), Xor(3)]
  let inverted = problem8.invert_cipher(ops)
  assert inverted == [Xor(3), Add(251)]
}

pub fn parse_cipher_spec_all_ops_test() {
  let spec = <<1, 2, 1, 3, 4, 2, 5, 0>>
  let assert Ok(#([ReverseBits, Xor(1), XorPos, Add(2), AddPos], <<>>)) =
    problem8.parse_cipher_spec(spec)
}

pub fn is_noop_complex_cipher_with_xor_zero_not_noop_test() {
  let ops = [
    Xor(226),
    AddPos,
    Add(208),
    Xor(245),
    Add(83),
    AddPos,
    Add(109),
    Xor(56),
    AddPos,
    Xor(247),
    Add(220),
    XorPos,
    XorPos,
    ReverseBits,
    AddPos,
    XorPos,
    ReverseBits,
    Add(19),
    XorPos,
    AddPos,
    XorPos,
    XorPos,
    AddPos,
    ReverseBits,
    Xor(99),
    XorPos,
    Add(236),
    Add(10),
    Xor(0),
    ReverseBits,
  ]
  assert !problem8.is_noop(ops)
}

pub fn multiple_lines_in_one_packet_test() {
  let ops = [Xor(1), ReverseBits]
  let inverse = problem8.invert_cipher(ops)
  let input = <<"3x foo,5x bar\n1x baz\n":utf8>>
  let #(encrypted, _) = problem8.transform_bytes(ops, input, 0)

  let #(decrypted, _) = problem8.transform_bytes(inverse, encrypted, 0)
  assert decrypted == input

  let assert Ok(text) = bit_array.to_string(decrypted)
  let #(lines, remainder) = problem8.process_line_buffer("", text)
  assert lines == ["3x foo,5x bar", "1x baz"]
  assert remainder == ""

  let responses = list.map(lines, problem8.handle_request)
  assert responses == ["5x bar", "1x baz"]
}
