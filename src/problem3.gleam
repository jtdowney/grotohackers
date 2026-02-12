import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import glisten.{Packet, User}

pub type RoomMessage {
  Join(
    name: String,
    subject: Subject(String),
    reply: Subject(Result(List(String), Nil)),
  )
  Leave(name: String)
  Chat(sender: String, text: String)
}

pub type ConnectionState {
  ConnectionState(
    buffer: String,
    name: Option(String),
    room: Subject(RoomMessage),
    client_subject: Subject(String),
  )
}

pub fn is_valid_name(name: String) -> Bool {
  use <- bool.guard(when: string.is_empty(name), return: False)
  name
  |> string.to_utf_codepoints
  |> list.all(is_alphanumeric)
}

fn is_alphanumeric(cp: UtfCodepoint) -> Bool {
  let n = string.utf_codepoint_to_int(cp)
  { n >= 0x30 && n <= 0x39 }
  || { n >= 0x41 && n <= 0x5A }
  || { n >= 0x61 && n <= 0x7A }
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

pub fn format_join_message(name: String) -> String {
  "* " <> name <> " has entered the room\n"
}

pub fn format_leave_message(name: String) -> String {
  "* " <> name <> " has left the room\n"
}

pub fn format_chat_message(name: String, text: String) -> String {
  "[" <> name <> "] " <> text <> "\n"
}

pub fn format_room_members(members: List(String)) -> String {
  "* The room contains: " <> string.join(members, ", ") <> "\n"
}

pub fn start_room() -> Subject(RoomMessage) {
  let assert Ok(started) =
    actor.new(dict.new())
    |> actor.on_message(handle_room_message)
    |> actor.start

  started.data
}

fn handle_room_message(
  state: Dict(String, Subject(String)),
  msg: RoomMessage,
) -> actor.Next(Dict(String, Subject(String)), RoomMessage) {
  case msg {
    Join(name:, subject:, reply:) -> {
      use <- bool.lazy_guard(when: dict.has_key(state, name), return: fn() {
        process.send(reply, Error(Nil))
        actor.continue(state)
      })

      let existing_names = dict.keys(state)
      let _ = broadcast(state, format_join_message(name))
      let new_state = dict.insert(state, name, subject)
      process.send(reply, Ok(existing_names))
      actor.continue(new_state)
    }
    Leave(name:) -> {
      let new_state = dict.delete(state, name)
      let _ = broadcast(new_state, format_leave_message(name))
      actor.continue(new_state)
    }
    Chat(sender:, text:) -> {
      let message = format_chat_message(sender, text)
      let _ = dict.delete(state, sender) |> broadcast(message)
      actor.continue(state)
    }
  }
}

fn broadcast(clients: Dict(String, Subject(String)), message: String) -> Nil {
  dict.each(clients, fn(_, subject) { process.send(subject, message) })
}

fn handle_connection(
  room: Subject(RoomMessage),
  conn: glisten.Connection(String),
) -> #(ConnectionState, Option(process.Selector(String))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  io.println(
    "New connection from "
    <> glisten.ip_address_to_string(ip_address)
    <> " on "
    <> int.to_string(port),
  )

  let client_subject = process.new_subject()
  let selector =
    process.new_selector()
    |> process.select(client_subject)

  let _ =
    glisten.send(
      conn,
      bytes_tree.from_string("Welcome to budgetchat! What shall I call you?\n"),
    )

  let state = ConnectionState(buffer: "", name: None, room:, client_subject:)
  #(state, Some(selector))
}

fn handle_client_data(
  state: ConnectionState,
  msg: glisten.Message(String),
  conn: glisten.Connection(String),
) -> glisten.Next(ConnectionState, glisten.Message(String)) {
  case msg {
    Packet(data) -> handle_packet(state, data, conn)
    User(text) -> {
      let _ = glisten.send(conn, bytes_tree.from_string(text))
      glisten.continue(state)
    }
  }
}

fn disconnect(
  conn: glisten.Connection(String),
  message: Option(String),
) -> glisten.Next(ConnectionState, glisten.Message(String)) {
  case message {
    Some(text) -> {
      let _ = glisten.send(conn, bytes_tree.from_string(text))
      Nil
    }
    None -> Nil
  }
  glisten.stop()
}

fn handle_packet(
  state: ConnectionState,
  data: BitArray,
  conn: glisten.Connection(String),
) -> glisten.Next(ConnectionState, glisten.Message(String)) {
  case bit_array.to_string(data) {
    Error(_) -> disconnect(conn, None)
    Ok(text) -> {
      let #(lines, new_buffer) = process_buffer(state.buffer, text)
      process_lines(ConnectionState(..state, buffer: new_buffer), lines, conn)
    }
  }
}

fn process_lines(
  state: ConnectionState,
  lines: List(String),
  conn: glisten.Connection(String),
) -> glisten.Next(ConnectionState, glisten.Message(String)) {
  case lines, state.name {
    [], _ -> glisten.continue(state)
    [line, ..rest], None -> handle_name_line(state, line, rest, conn)
    [line, ..rest], Some(name) -> {
      process.send(state.room, Chat(sender: name, text: line))
      process_lines(state, rest, conn)
    }
  }
}

fn handle_name_line(
  state: ConnectionState,
  name: String,
  remaining_lines: List(String),
  conn: glisten.Connection(String),
) -> glisten.Next(ConnectionState, glisten.Message(String)) {
  use <- bool.lazy_guard(when: !is_valid_name(name), return: fn() {
    disconnect(conn, Some("Invalid name. Disconnecting.\n"))
  })

  let result =
    process.call(state.room, 5000, fn(reply) {
      Join(name:, subject: state.client_subject, reply:)
    })
  case result {
    Error(Nil) -> disconnect(conn, Some("Name already taken. Disconnecting.\n"))
    Ok(members) -> {
      let _ =
        glisten.send(conn, bytes_tree.from_string(format_room_members(members)))
      let new_state = ConnectionState(..state, name: Some(name))
      process_lines(new_state, remaining_lines, conn)
    }
  }
}

fn handle_close(state: ConnectionState) -> Nil {
  case state.name {
    None -> Nil
    Some(name) -> process.send(state.room, Leave(name:))
  }
}

pub fn main() -> Nil {
  let room = start_room()
  let assert Ok(_) =
    glisten.new(handle_connection(room, _), handle_client_data)
    |> glisten.with_close(handle_close)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
