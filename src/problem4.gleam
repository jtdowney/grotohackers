import gleam/bit_array
import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Selector}
import gleam/int
import gleam/io
import gleam/option.{type Option}
import gleam/result
import gleam/string
import grammy

pub type Request {
  Insert(key: String, value: String)
  Retrieve(key: String)
}

pub fn parse_request(data: BitArray) -> Result(Request, Nil) {
  use text <- result.try(bit_array.to_string(data))
  case string.split_once(text, "=") {
    Ok(#(key, value)) -> Ok(Insert(key:, value:))
    Error(_) -> Ok(Retrieve(key: text))
  }
}

pub fn handle_request(
  store: Dict(String, String),
  request: Request,
) -> #(Dict(String, String), Result(String, Nil)) {
  case request {
    Insert("version", _) -> #(store, Error(Nil))
    Insert(key:, value:) -> #(dict.insert(store, key, value), Error(Nil))
    Retrieve(key:) -> {
      let value = dict.get(store, key) |> result.unwrap("")
      #(store, Ok(key <> "=" <> value))
    }
  }
}

fn handle_packet(store, conn, address, port, data) {
  case parse_request(data) {
    Error(_) -> store
    Ok(request) -> {
      let #(new_store, response) = handle_request(store, request)
      let _ =
        result.map(response, fn(text) {
          grammy.send_to(conn, address, port, bytes_tree.from_string(text))
        })
      new_store
    }
  }
}

fn handle_connection() -> #(Dict(String, String), Option(Selector(Nil))) {
  let store = dict.from_list([#("version", "grotohackers 1.0")])
  #(store, option.None)
}

fn handle_client_data(
  msg: grammy.Message(Nil),
  conn: grammy.Connection,
  store: Dict(String, String),
) -> grammy.Next(Dict(String, String), Nil) {
  case msg {
    grammy.Packet(address, port, data) -> {
      io.println(
        "Packet from "
        <> grammy.ip_address_to_string(address)
        <> ":"
        <> int.to_string(port),
      )
      handle_packet(store, conn, address, port, data)
      |> grammy.continue
    }
    grammy.User(_) -> grammy.continue(store)
  }
}

pub fn main() -> Nil {
  let assert Ok(_) =
    grammy.new(handle_connection, handle_client_data)
    |> grammy.port(3050)
    |> grammy.start

  process.sleep_forever()
}
