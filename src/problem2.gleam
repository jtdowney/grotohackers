import bitty
import bitty/bytes
import bitty/num.{BigEndian}
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/result
import glisten.{Packet}
import logging

pub type Message {
  Insert(timestamp: Int, price: Int)
  Query(mintime: Int, maxtime: Int)
}

pub type State {
  State(buffer: BitArray, prices: List(#(Int, Int)))
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
  conn: glisten.Connection(Message),
) -> #(State, Option(process.Selector(Message))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  logging.log(
    logging.Debug,
    "New connection from "
      <> glisten.ip_address_to_string(ip_address)
      <> " on "
      <> int.to_string(port),
  )

  #(State(buffer: <<>>, prices: []), option.None)
}

fn handle_client_data(
  state: State,
  msg: glisten.Message(Message),
  conn: glisten.Connection(Message),
) -> glisten.Next(State, glisten.Message(Message)) {
  let assert Packet(data) = msg
  let State(buffer:, prices:) = state

  case process_buffer(buffer, data) {
    Error(_) -> glisten.stop()
    Ok(#(messages, new_buffer)) -> {
      let #(new_prices, responses) = process_messages(prices, messages)
      list.each(responses, fn(response) {
        let _ = glisten.send(conn, bytes_tree.from_bit_array(response))
      })
      glisten.continue(State(buffer: new_buffer, prices: new_prices))
    }
  }
}

fn insert_parser() -> bitty.Parser(Message) {
  use #(ts, price) <- bitty.then(bitty.preceded(
    bytes.tag(<<73>>),
    bitty.pair(num.i32(BigEndian), num.i32(BigEndian)),
  ))
  bitty.success(Insert(timestamp: ts, price: price))
}

fn query_parser() -> bitty.Parser(Message) {
  use #(min, max) <- bitty.then(bitty.preceded(
    bytes.tag(<<81>>),
    bitty.pair(num.i32(BigEndian), num.i32(BigEndian)),
  ))
  bitty.success(Query(mintime: min, maxtime: max))
}

fn message_parser() -> bitty.Parser(Message) {
  bitty.one_of([insert_parser(), query_parser()])
}

pub fn parse_message(data: BitArray) -> Result(Message, Nil) {
  bitty.run(message_parser(), on: data)
  |> result.replace_error(Nil)
}

pub fn process_buffer(
  buffer: BitArray,
  new_data: BitArray,
) -> Result(#(List(Message), BitArray), Nil) {
  let full = bit_array.append(buffer, new_data)
  extract_messages(full, [])
}

fn extract_messages(
  data: BitArray,
  acc: List(Message),
) -> Result(#(List(Message), BitArray), Nil) {
  case bit_array.byte_size(data) < 9 {
    True -> Ok(#(list.reverse(acc), data))
    False -> {
      case bitty.run_partial(message_parser(), on: data) {
        Error(_) -> Error(Nil)
        Ok(#(msg, rest)) -> extract_messages(rest, [msg, ..acc])
      }
    }
  }
}

pub fn compute_mean(
  prices: List(#(Int, Int)),
  mintime: Int,
  maxtime: Int,
) -> Int {
  use <- bool.guard(when: mintime > maxtime, return: 0)

  let matching =
    list.filter(prices, fn(entry) {
      let #(ts, _) = entry
      ts >= mintime && ts <= maxtime
    })

  let count = list.length(matching)
  use <- bool.guard(when: count == 0, return: 0)

  let sum =
    list.fold(matching, 0, fn(acc, entry) {
      let #(_, price) = entry
      acc + price
    })
  sum / count
}

pub fn process_messages(
  prices: List(#(Int, Int)),
  messages: List(Message),
) -> #(List(#(Int, Int)), List(BitArray)) {
  let #(final_prices, reversed_responses) =
    list.fold(messages, #(prices, []), fn(acc, msg) {
      let #(current_prices, responses) = acc
      case msg {
        Insert(timestamp:, price:) -> #(
          [#(timestamp, price), ..current_prices],
          responses,
        )
        Query(mintime:, maxtime:) -> {
          let mean = compute_mean(current_prices, mintime, maxtime)
          #(current_prices, [<<mean:size(32)-big>>, ..responses])
        }
      }
    })
  #(final_prices, list.reverse(reversed_responses))
}
