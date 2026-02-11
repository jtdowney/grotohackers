import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option}
import gleam/result
import glisten.{Packet}

pub type Message {
  Insert(timestamp: Int, price: Int)
  Query(mintime: Int, maxtime: Int)
}

pub type State {
  State(buffer: BitArray, prices: List(#(Int, Int)))
}

pub fn main() -> Nil {
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
  io.println(
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

pub fn parse_message(data: BitArray) -> Result(Message, Nil) {
  case data {
    <<73:8, ts:size(32)-big-signed, price:size(32)-big-signed>> ->
      Ok(Insert(timestamp: ts, price: price))
    <<81:8, min:size(32)-big-signed, max:size(32)-big-signed>> ->
      Ok(Query(mintime: min, maxtime: max))
    _ -> Error(Nil)
  }
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
  case data {
    <<chunk:bytes-size(9), rest:bytes>> -> {
      use msg <- result.try(parse_message(chunk))
      extract_messages(rest, [msg, ..acc])
    }
    _ -> Ok(#(list.reverse(acc), data))
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
