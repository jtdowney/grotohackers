import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None}
import gleam/result
import gleam/string
import glisten.{Packet}
import logging

pub type CipherOp {
  ReverseBits
  Xor(n: Int)
  XorPos
  Add(n: Int)
  AddPos
  SubPos
}

pub type ClientState {
  ClientState(
    encode_ops: List(CipherOp),
    decode_ops: List(CipherOp),
    encode_pos: Int,
    decode_pos: Int,
  )
}

pub type Phase {
  NegotiatingCipher(spec_buffer: BitArray)
  Operating(client: ClientState, line_buffer: String)
}

pub fn reverse_bits(byte: Int) -> Int {
  int.range(from: 0, to: 8, with: 0, run: fn(acc, i) {
    let bit = int.bitwise_and(int.bitwise_shift_right(byte, i), 1)
    int.bitwise_or(acc, int.bitwise_shift_left(bit, 7 - i))
  })
}

pub fn apply_op(op: CipherOp, byte: Int, pos: Int) -> Int {
  case op {
    ReverseBits -> reverse_bits(byte)
    Xor(n) -> int.bitwise_and(int.bitwise_exclusive_or(byte, n), 0xff)
    XorPos -> int.bitwise_and(int.bitwise_exclusive_or(byte, pos % 256), 0xff)
    Add(n) -> int.bitwise_and(byte + n, 0xff)
    AddPos -> int.bitwise_and(byte + pos % 256, 0xff)
    SubPos -> int.bitwise_and(byte - pos % 256 + 256, 0xff)
  }
}

pub fn apply_cipher(ops: List(CipherOp), byte: Int, pos: Int) -> Int {
  list.fold(ops, byte, fn(b, op) { apply_op(op, b, pos) })
}

pub fn transform_bytes(
  ops: List(CipherOp),
  data: BitArray,
  start_pos: Int,
) -> #(BitArray, Int) {
  transform_bytes_loop(ops, data, start_pos, <<>>)
}

fn transform_bytes_loop(
  ops: List(CipherOp),
  data: BitArray,
  pos: Int,
  acc: BitArray,
) -> #(BitArray, Int) {
  case data {
    <<byte:8, rest:bytes>> -> {
      let transformed = apply_cipher(ops, byte, pos)
      transform_bytes_loop(ops, rest, pos + 1, <<acc:bits, transformed:8>>)
    }
    _ -> #(acc, pos)
  }
}

pub fn parse_cipher_spec(
  data: BitArray,
) -> Result(#(List(CipherOp), BitArray), Nil) {
  parse_cipher_spec_loop(data, [])
}

fn parse_cipher_spec_loop(
  data: BitArray,
  acc: List(CipherOp),
) -> Result(#(List(CipherOp), BitArray), Nil) {
  case data {
    <<0:8, rest:bytes>> -> Ok(#(list.reverse(acc), rest))
    <<1:8, rest:bytes>> -> parse_cipher_spec_loop(rest, [ReverseBits, ..acc])
    <<2:8, n:8, rest:bytes>> -> parse_cipher_spec_loop(rest, [Xor(n), ..acc])
    <<3:8, rest:bytes>> -> parse_cipher_spec_loop(rest, [XorPos, ..acc])
    <<4:8, n:8, rest:bytes>> -> parse_cipher_spec_loop(rest, [Add(n), ..acc])
    <<5:8, rest:bytes>> -> parse_cipher_spec_loop(rest, [AddPos, ..acc])
    _ -> Error(Nil)
  }
}

pub fn invert_op(op: CipherOp) -> CipherOp {
  case op {
    ReverseBits -> ReverseBits
    Xor(n) -> Xor(n)
    XorPos -> XorPos
    Add(n) -> Add(int.bitwise_and(256 - n, 0xff))
    AddPos -> SubPos
    SubPos -> AddPos
  }
}

pub fn invert_cipher(ops: List(CipherOp)) -> List(CipherOp) {
  ops
  |> list.reverse
  |> list.map(invert_op)
}

pub fn is_noop(ops: List(CipherOp)) -> Bool {
  is_noop_at_pos(ops, 0, 0) && is_noop_at_pos(ops, 1, 0)
}

fn is_noop_at_pos(ops: List(CipherOp), pos: Int, byte: Int) -> Bool {
  use <- bool.guard(when: byte > 255, return: True)
  use <- bool.guard(when: apply_cipher(ops, byte, pos) != byte, return: False)
  is_noop_at_pos(ops, pos, byte + 1)
}

pub fn process_line_buffer(
  buffer: String,
  new_data: String,
) -> #(List(String), String) {
  let full_buffer = buffer <> new_data
  let parts = string.split(full_buffer, "\n")

  let assert [remainder, ..complete_reversed] = list.reverse(parts)
  #(list.reverse(complete_reversed), remainder)
}

pub fn parse_toys(line: String) -> List(#(Int, String)) {
  line
  |> string.split(",")
  |> list.filter_map(fn(entry) {
    use #(count_str, name) <- result.try(string.split_once(
      string.trim(entry),
      "x ",
    ))
    use count <- result.try(int.parse(count_str))
    Ok(#(count, name))
  })
}

pub fn find_max_toy(toys: List(#(Int, String))) -> Result(String, Nil) {
  list.reduce(toys, fn(best, toy) {
    let #(best_count, _) = best
    let #(count, _) = toy
    case count > best_count {
      True -> toy
      False -> best
    }
  })
  |> result.map(fn(max) {
    let #(max_count, max_name) = max
    int.to_string(max_count) <> "x " <> max_name
  })
}

pub fn handle_request(line: String) -> String {
  let toys = parse_toys(line)
  find_max_toy(toys)
  |> result.unwrap("")
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let assert Ok(_) =
    glisten.new(handle_connection, handle_client_data)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}

fn handle_connection(
  conn: glisten.Connection(Nil),
) -> #(Phase, option.Option(process.Selector(Nil))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  logging.log(
    logging.Debug,
    "New connection from "
      <> glisten.ip_address_to_string(ip_address)
      <> " on "
      <> int.to_string(port),
  )

  #(NegotiatingCipher(spec_buffer: <<>>), None)
}

fn handle_client_data(
  phase: Phase,
  msg: glisten.Message(Nil),
  conn: glisten.Connection(Nil),
) -> glisten.Next(Phase, glisten.Message(Nil)) {
  case msg {
    Packet(data) ->
      case phase {
        NegotiatingCipher(spec_buffer:) ->
          handle_negotiation(spec_buffer, data, conn)
        Operating(client:, line_buffer:) ->
          handle_operating(client, line_buffer, data, conn)
      }
    _ -> glisten.stop()
  }
}

fn handle_negotiation(
  spec_buffer: BitArray,
  data: BitArray,
  conn: glisten.Connection(Nil),
) -> glisten.Next(Phase, glisten.Message(Nil)) {
  let full = bit_array.append(spec_buffer, data)

  case parse_cipher_spec(full) {
    Error(_) -> glisten.continue(NegotiatingCipher(spec_buffer: full))
    Ok(#(encode_ops, remaining)) -> {
      use <- bool.guard(when: is_noop(encode_ops), return: glisten.stop())

      let decode_ops = invert_cipher(encode_ops)

      let client =
        ClientState(encode_ops:, decode_ops:, encode_pos: 0, decode_pos: 0)

      use <- bool.guard(
        when: bit_array.byte_size(remaining) == 0,
        return: glisten.continue(Operating(client:, line_buffer: "")),
      )
      handle_operating(client, "", remaining, conn)
    }
  }
}

fn send_responses(
  conn: glisten.Connection(Nil),
  client: ClientState,
  lines: List(String),
) -> Int {
  list.fold(lines, client.encode_pos, fn(pos, line) {
    let response = handle_request(line) <> "\n"
    let response_bytes = bit_array.from_string(response)
    let #(encrypted, next_pos) =
      transform_bytes(client.encode_ops, response_bytes, pos)
    let _ = glisten.send(conn, bytes_tree.from_bit_array(encrypted))
    next_pos
  })
}

fn handle_operating(
  client: ClientState,
  line_buffer: String,
  data: BitArray,
  conn: glisten.Connection(Nil),
) -> glisten.Next(Phase, glisten.Message(Nil)) {
  let #(decrypted_bytes, new_decode_pos) =
    transform_bytes(client.decode_ops, data, client.decode_pos)

  case bit_array.to_string(decrypted_bytes) {
    Error(_) -> glisten.stop()
    Ok(text) -> {
      let #(lines, new_buffer) = process_line_buffer(line_buffer, text)
      let new_encode_pos = send_responses(conn, client, lines)

      let new_client =
        ClientState(
          ..client,
          encode_pos: new_encode_pos,
          decode_pos: new_decode_pos,
        )
      glisten.continue(Operating(client: new_client, line_buffer: new_buffer))
    }
  }
}
