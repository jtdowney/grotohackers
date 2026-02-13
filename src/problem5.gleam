import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/string
import glisten.{Packet, User}
import mug

const tony_address = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

const upstream_host = "chat.protohackers.com"

const upstream_port = 16_963

pub type ProxyMessage {
  UpstreamPacket(BitArray)
  UpstreamClosed
  UpstreamError(mug.Error)
}

pub type State {
  State(
    client_buffer: String,
    upstream_buffer: String,
    upstream_socket: mug.Socket,
  )
}

pub fn process_buffer(
  buffer: String,
  new_data: String,
) -> #(List(String), String) {
  let full_buffer = buffer <> new_data
  let parts = string.split(full_buffer, "\n")

  case list.reverse(parts) {
    [] -> #([], "")
    [remainder, ..complete_reversed] -> {
      let lines =
        list.reverse(complete_reversed)
        |> list.map(string.replace(_, "\r", ""))
      #(lines, remainder)
    }
  }
}

pub fn is_boguscoin_address(word: String) -> Bool {
  let length = string.length(word)
  use <- bool.guard(when: length < 26 || length > 35, return: False)
  use <- bool.guard(when: !string.starts_with(word, "7"), return: False)
  word
  |> string.to_utf_codepoints
  |> list.all(is_alphanumeric)
}

fn is_alphanumeric(cp: UtfCodepoint) -> Bool {
  let n = string.utf_codepoint_to_int(cp)
  { n >= 0x30 && n <= 0x39 }
  || { n >= 0x41 && n <= 0x5A }
  || { n >= 0x61 && n <= 0x7A }
}

pub fn rewrite_boguscoin(line: String) -> String {
  line
  |> string.split(" ")
  |> list.map(fn(word) {
    case is_boguscoin_address(word) {
      True -> tony_address
      False -> word
    }
  })
  |> string.join(" ")
}

fn rewrite_lines(lines: List(String)) -> String {
  lines
  |> list.map(rewrite_boguscoin)
  |> list.map(fn(line) { line <> "\n" })
  |> string.join("")
}

fn upstream_receive_loop(
  socket: mug.Socket,
  subject: process.Subject(ProxyMessage),
) -> Nil {
  case mug.receive(socket, timeout_milliseconds: 30_000) {
    Ok(data) -> {
      process.send(subject, UpstreamPacket(data))
      upstream_receive_loop(socket, subject)
    }
    Error(mug.Timeout) -> upstream_receive_loop(socket, subject)
    Error(mug.Closed) -> process.send(subject, UpstreamClosed)
    Error(err) -> process.send(subject, UpstreamError(err))
  }
}

fn handle_connection(
  conn: glisten.Connection(ProxyMessage),
) -> #(State, option.Option(process.Selector(ProxyMessage))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  io.println(
    "New connection from "
    <> glisten.ip_address_to_string(ip_address)
    <> " on "
    <> int.to_string(port),
  )

  let assert Ok(upstream_socket) =
    mug.new(upstream_host, port: upstream_port)
    |> mug.timeout(milliseconds: 5000)
    |> mug.connect

  let upstream_subject = process.new_subject()
  let _ =
    process.spawn(fn() {
      upstream_receive_loop(upstream_socket, upstream_subject)
    })

  let selector =
    process.new_selector()
    |> process.select(upstream_subject)

  let state = State(client_buffer: "", upstream_buffer: "", upstream_socket:)
  #(state, Some(selector))
}

fn handle_client_data(
  state: State,
  msg: glisten.Message(ProxyMessage),
  conn: glisten.Connection(ProxyMessage),
) -> glisten.Next(State, glisten.Message(ProxyMessage)) {
  case msg {
    Packet(data) -> handle_client_packet(state, data)
    User(UpstreamPacket(data)) -> handle_upstream_packet(state, data, conn)
    User(UpstreamClosed) | User(UpstreamError(_)) -> glisten.stop()
  }
}

fn handle_client_packet(
  state: State,
  data: BitArray,
) -> glisten.Next(State, glisten.Message(ProxyMessage)) {
  case bit_array.to_string(data) {
    Error(_) -> glisten.stop()
    Ok(text) -> {
      let #(lines, new_buffer) = process_buffer(state.client_buffer, text)
      let rewritten = rewrite_lines(lines)
      use <- bool.guard(when: string.is_empty(rewritten), return: {
        glisten.continue(State(..state, client_buffer: new_buffer))
      })
      let assert Ok(_) =
        mug.send_builder(
          state.upstream_socket,
          bytes_tree.from_string(rewritten),
        )
      glisten.continue(State(..state, client_buffer: new_buffer))
    }
  }
}

fn handle_upstream_packet(
  state: State,
  data: BitArray,
  conn: glisten.Connection(ProxyMessage),
) -> glisten.Next(State, glisten.Message(ProxyMessage)) {
  case bit_array.to_string(data) {
    Error(_) -> glisten.stop()
    Ok(text) -> {
      let #(lines, new_buffer) = process_buffer(state.upstream_buffer, text)
      let rewritten = rewrite_lines(lines)
      use <- bool.guard(when: string.is_empty(rewritten), return: {
        glisten.continue(State(..state, upstream_buffer: new_buffer))
      })
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(rewritten))
      glisten.continue(State(..state, upstream_buffer: new_buffer))
    }
  }
}

fn handle_close(state: State) -> Nil {
  let _ = mug.shutdown(state.upstream_socket)
  Nil
}

pub fn main() -> Nil {
  let assert Ok(_) =
    glisten.new(handle_connection, handle_client_data)
    |> glisten.with_close(handle_close)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
