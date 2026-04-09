import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/result
import gleam/string
import glip
import logging
import toss

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

fn handle_packet(
  store: Dict(String, String),
  socket: toss.Socket,
  address: glip.IpAddress,
  port: Int,
  data: BitArray,
) -> Dict(String, String) {
  case parse_request(data) {
    Error(_) -> store
    Ok(request) -> {
      let #(new_store, response) = handle_request(store, request)
      let _ =
        result.map(response, fn(text) {
          toss.send_to(socket, address, port, bit_array.from_string(text))
        })
      new_store
    }
  }
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let assert Ok(socket) =
    toss.new(port: 3050)
    |> toss.use_ipv4()
    |> toss.open()

  let store = dict.from_list([#("version", "grotohackers 1.0")])
  let selector =
    process.new_selector()
    |> toss.select_udp_messages(function.identity)

  let assert Ok(_) = toss.receive_next_datagram_as_message(socket)
  server_loop(socket, selector, store)
}

fn server_loop(
  socket: toss.Socket,
  selector: process.Selector(toss.UdpMessage),
  store: Dict(String, String),
) -> Nil {
  let msg = process.selector_receive_forever(from: selector)
  let assert Ok(_) = toss.receive_next_datagram_as_message(socket)
  case msg {
    toss.Datagram(_, host, port, data) -> {
      let store = case host {
        Ok(address) -> {
          logging.log(
            logging.Debug,
            "Packet from "
              <> glip.ip_to_string(address)
              <> ":"
              <> int.to_string(port),
          )
          handle_packet(store, socket, address, port, data)
        }
        Error(_) -> store
      }
      server_loop(socket, selector, store)
    }
    toss.UdpError(_, _) -> server_loop(socket, selector, store)
  }
}
