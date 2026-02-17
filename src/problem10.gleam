import bitty
import bitty/string as s
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/crypto
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/set.{type Set}
import gleam/string
import glisten
import logging

pub type Command {
  Help
  Get(file: String, revision: Option(Int))
  Put(file: String, length: Int)
  List(directory: String)
}

pub type VcsMessage {
  HandleGet(
    file: String,
    revision: Option(Int),
    reply: Subject(Result(BitArray, String)),
  )
  HandlePut(file: String, data: BitArray, reply: Subject(Result(Int, String)))
  HandleList(directory: String, reply: Subject(List(String)))
}

pub type VcsState {
  VcsState(
    blobs: Dict(BitArray, BitArray),
    files: Dict(String, List(BitArray)),
    directories: Dict(String, Set(String)),
  )
}

pub type ConnectionMode {
  AwaitingCommand
  AwaitingData(file: String, remaining: Int, accumulated: BitArray)
}

pub type ConnectionState {
  ConnectionState(
    buffer: BitArray,
    mode: ConnectionMode,
    vcs: Subject(VcsMessage),
  )
}

const illegal_chars = "`~!@#$%^&*()+={}[]:;'\",?\\|"

pub fn is_valid_filename(name: String) -> Bool {
  use <- bool.guard(when: !string.starts_with(name, "/"), return: False)

  let illegal_set =
    illegal_chars
    |> string.to_graphemes

  name
  |> string.to_graphemes
  |> list.all(fn(c) { !list.contains(illegal_set, c) && !is_whitespace(c) })
}

fn is_whitespace(c: String) -> Bool {
  case c {
    " " | "\t" | "\n" | "\r" -> True
    _ -> False
  }
}

pub fn is_valid_data(data: BitArray) -> Bool {
  case bit_array.to_string(data) {
    Error(_) -> False
    Ok(s) ->
      s
      |> string.to_utf_codepoints
      |> list.all(fn(cp) {
        let n = string.utf_codepoint_to_int(cp)
        is_ascii_graphic(n) || is_ascii_whitespace(n)
      })
  }
}

fn is_ascii_graphic(n: Int) -> Bool {
  n >= 0x21 && n <= 0x7E
}

fn is_ascii_whitespace(n: Int) -> Bool {
  n == 0x20 || n == 0x09 || n == 0x0A || n == 0x0D
}

pub fn parse_command(line: String) -> Result(Command, String) {
  let trimmed = string.trim_end(line)
  let input = bit_array.from_string(trimmed)

  bitty.run(command_parser(), on: input)
  |> result.map_error(fn(_) { command_error(trimmed) })
}

fn command_error(line: String) -> String {
  let first_word =
    line
    |> string.trim
    |> string.split(" ")
    |> list.first
    |> result.unwrap("")
    |> string.lowercase

  case first_word {
    "" -> "illegal method: "
    "get" -> "usage: GET file [revision]"
    "put" -> "usage: PUT file length newline data"
    "list" -> "usage: LIST dir"
    other -> "illegal method: " <> other
  }
}

fn command_parser() -> bitty.Parser(Command) {
  bitty.one_of([
    bitty.attempt(help_parser()),
    bitty.attempt(get_parser()),
    bitty.attempt(put_parser()),
    list_parser(),
  ])
}

fn command_token(name: String) -> bitty.Parser(Nil) {
  s.alpha1()
  |> bitty.verify(fn(word) { string.lowercase(word) == name })
  |> bitty.replace(Nil)
}

fn sp() -> bitty.Parser(Nil) {
  s.space1() |> bitty.replace(Nil)
}

fn help_parser() -> bitty.Parser(Command) {
  use _ <- bitty.then(command_token("help"))
  use _ <- bitty.then(bitty.end())
  bitty.success(Help)
}

fn non_whitespace_token() -> bitty.Parser(String) {
  s.take_while1(fn(g) { !is_whitespace(g) })
}

fn filename_token() -> bitty.Parser(String) {
  non_whitespace_token()
  |> bitty.verify(is_valid_filename)
}

fn revision_token() -> bitty.Parser(Int) {
  use _ <- bitty.then(s.literal("r"))
  s.integer()
}

fn get_parser() -> bitty.Parser(Command) {
  use _ <- bitty.then(command_token("get"))
  use _ <- bitty.then(sp())
  use file <- bitty.then(filename_token())
  use rev <- bitty.then(
    bitty.one_of([
      bitty.attempt({
        use _ <- bitty.then(sp())
        use r <- bitty.then(revision_token())
        use _ <- bitty.then(bitty.end())
        bitty.success(Some(r))
      }),
      {
        use _ <- bitty.then(bitty.end())
        bitty.success(None)
      },
    ]),
  )
  bitty.success(Get(file:, revision: rev))
}

fn put_parser() -> bitty.Parser(Command) {
  use _ <- bitty.then(command_token("put"))
  use _ <- bitty.then(sp())
  use file <- bitty.then(filename_token())
  use _ <- bitty.then(sp())
  use length <- bitty.then(s.integer())
  use _ <- bitty.then(bitty.end())
  bitty.success(Put(file:, length:))
}

fn list_parser() -> bitty.Parser(Command) {
  use _ <- bitty.then(command_token("list"))
  use _ <- bitty.then(sp())
  use dir <- bitty.then(filename_token())
  use _ <- bitty.then(bitty.end())
  bitty.success(List(directory: dir))
}

pub fn start_vcs() -> Subject(VcsMessage) {
  let assert Ok(started) =
    actor.new(VcsState(
      blobs: dict.new(),
      files: dict.new(),
      directories: dict.from_list([#("/", set.new())]),
    ))
    |> actor.on_message(handle_vcs_message)
    |> actor.start

  started.data
}

fn handle_vcs_message(
  state: VcsState,
  msg: VcsMessage,
) -> actor.Next(VcsState, VcsMessage) {
  case msg {
    HandleGet(file:, revision:, reply:) -> {
      let result = vcs_get(state, file, revision)
      process.send(reply, result)
      actor.continue(state)
    }
    HandlePut(file:, data:, reply:) -> {
      let #(new_state, result) = vcs_put(state, file, data)
      process.send(reply, result)
      actor.continue(new_state)
    }
    HandleList(directory:, reply:) -> {
      let entries = vcs_list(state, directory)
      process.send(reply, entries)
      actor.continue(state)
    }
  }
}

fn vcs_get(
  state: VcsState,
  file: String,
  revision: Option(Int),
) -> Result(BitArray, String) {
  use revisions <- result.try(
    dict.get(state.files, file)
    |> result.replace_error("no such file"),
  )

  let rev_num = case revision {
    None -> list.length(revisions)
    Some(r) -> r
  }

  use <- bool.guard(when: rev_num < 1, return: Error("no such file"))

  use hash <- result.try(
    revisions
    |> list.drop(rev_num - 1)
    |> list.first
    |> result.replace_error("no such file"),
  )

  dict.get(state.blobs, hash)
  |> result.replace_error("no such file")
}

fn split_path(file: String) -> List(String) {
  file
  |> string.split("/")
  |> list.filter(fn(s) { s != "" })
}

fn vcs_put(
  state: VcsState,
  file: String,
  data: BitArray,
) -> #(VcsState, Result(Int, String)) {
  let parts = split_path(file)

  use <- bool.lazy_guard(when: list.is_empty(parts), return: fn() {
    #(state, Error("illegal file name"))
  })

  use <- bool.lazy_guard(when: !is_valid_data(data), return: fn() {
    #(state, Error("invalid data"))
  })

  let hash = crypto.hash(crypto.Sha256, data)
  let blobs = case dict.has_key(state.blobs, hash) {
    True -> state.blobs
    False -> dict.insert(state.blobs, hash, data)
  }

  let #(directories, dir_path) = ensure_directories(state.directories, file)

  let assert Ok(filename) = list.last(parts)

  let dir_entries =
    dict.get(directories, dir_path)
    |> result.unwrap(set.new())
    |> set.insert(filename)
  let directories = dict.insert(directories, dir_path, dir_entries)

  let revisions =
    dict.get(state.files, file)
    |> result.unwrap([])
  let revisions = case list.last(revisions) {
    Ok(last_hash) if last_hash == hash -> revisions
    _ -> list.append(revisions, [hash])
  }
  let files = dict.insert(state.files, file, revisions)
  let rev_num = list.length(revisions)

  #(VcsState(blobs:, files:, directories:), Ok(rev_num))
}

fn ensure_directories(
  directories: Dict(String, Set(String)),
  file: String,
) -> #(Dict(String, Set(String)), String) {
  let parts = split_path(file)

  let assert Ok(dir_parts) =
    list.rest(list.reverse(parts)) |> result.map(list.reverse)

  build_directory_path(directories, "/", dir_parts)
}

fn build_directory_path(
  directories: Dict(String, Set(String)),
  current_path: String,
  parts: List(String),
) -> #(Dict(String, Set(String)), String) {
  case parts {
    [] -> #(directories, current_path)
    [part, ..rest] -> {
      let entries =
        dict.get(directories, current_path)
        |> result.unwrap(set.new())
        |> set.insert(part)
      let directories = dict.insert(directories, current_path, entries)

      let next_path = current_path <> part <> "/"
      let directories = case dict.has_key(directories, next_path) {
        True -> directories
        False -> dict.insert(directories, next_path, set.new())
      }

      build_directory_path(directories, next_path, rest)
    }
  }
}

pub fn vcs_list(state: VcsState, directory: String) -> List(String) {
  let dir = case string.ends_with(directory, "/") {
    True -> directory
    False -> directory <> "/"
  }

  let entries =
    dict.get(state.directories, dir)
    |> result.unwrap(set.new())

  entries
  |> set.to_list
  |> list.filter_map(fn(name) {
    let file_path = dir <> name
    let file_entry =
      dict.get(state.files, file_path)
      |> result.map(fn(revisions) {
        name <> " r" <> int.to_string(list.length(revisions))
      })

    let dir_path = file_path <> "/"
    let dir_entry =
      dict.get(state.directories, dir_path)
      |> result.map(fn(_) { name <> "/ DIR" })

    case file_entry, dir_entry {
      Ok(f), _ -> Ok(f)
      _, Ok(d) -> Ok(d)
      _, _ -> Error(Nil)
    }
  })
  |> list.sort(string.compare)
}

fn handle_connection(
  vcs: Subject(VcsMessage),
  conn: glisten.Connection(Nil),
) -> #(ConnectionState, Option(process.Selector(Nil))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  logging.log(
    logging.Debug,
    "New connection from "
      <> glisten.ip_address_to_string(ip_address)
      <> " on "
      <> int.to_string(port),
  )

  let _ = glisten.send(conn, bytes_tree.from_string("READY\n"))

  let state = ConnectionState(buffer: <<>>, mode: AwaitingCommand, vcs:)
  #(state, None)
}

fn handle_tcp_message(
  state: ConnectionState,
  msg: glisten.Message(Nil),
  conn: glisten.Connection(Nil),
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  case msg {
    glisten.Packet(data) -> handle_packet(state, data, conn)
    glisten.User(_) -> glisten.continue(state)
  }
}

fn handle_packet(
  state: ConnectionState,
  data: BitArray,
  conn: glisten.Connection(Nil),
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  let buffer = bit_array.append(state.buffer, data)
  let state = ConnectionState(..state, buffer:)
  process_buffer(state, conn)
}

fn process_buffer(
  state: ConnectionState,
  conn: glisten.Connection(Nil),
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  case state.mode {
    AwaitingCommand -> process_command_buffer(state, conn)
    AwaitingData(file:, remaining:, accumulated:) ->
      process_data_buffer(state, conn, file, remaining, accumulated)
  }
}

fn process_command_buffer(
  state: ConnectionState,
  conn: glisten.Connection(Nil),
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  case find_newline(state.buffer) {
    Error(Nil) -> glisten.continue(state)
    Ok(#(line_bytes, rest)) -> {
      let state = ConnectionState(..state, buffer: rest)
      case bit_array.to_string(line_bytes) {
        Error(_) -> {
          send_response(conn, "ERR invalid command\n")
          process_buffer(state, conn)
        }
        Ok(line) -> dispatch_command(state, conn, line)
      }
    }
  }
}

fn dispatch_command(
  state: ConnectionState,
  conn: glisten.Connection(Nil),
  line: String,
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  case parse_command(line) {
    Error(msg) -> {
      send_response(conn, "ERR " <> msg <> "\n")
      send_response(conn, "READY\n")
      process_buffer(state, conn)
    }
    Ok(Help) -> {
      send_response(conn, "OK usage: HELP|GET|PUT|LIST\n")
      send_response(conn, "READY\n")
      process_buffer(state, conn)
    }
    Ok(Get(file:, revision:)) -> handle_get_command(state, conn, file, revision)
    Ok(Put(file:, length:)) -> {
      let state =
        ConnectionState(
          ..state,
          mode: AwaitingData(file:, remaining: length, accumulated: <<>>),
        )
      process_buffer(state, conn)
    }
    Ok(List(directory:)) -> handle_list_command(state, conn, directory)
  }
}

fn handle_get_command(
  state: ConnectionState,
  conn: glisten.Connection(Nil),
  file: String,
  revision: Option(Int),
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  let result =
    process.call(state.vcs, 5000, fn(reply) {
      HandleGet(file:, revision:, reply:)
    })
  case result {
    Error(msg) -> send_response(conn, "ERR " <> msg <> "\n")
    Ok(data) -> {
      let len = bit_array.byte_size(data)
      send_response(conn, "OK " <> int.to_string(len) <> "\n")
      let _ = glisten.send(conn, bytes_tree.from_bit_array(data))
      Nil
    }
  }
  send_response(conn, "READY\n")
  process_buffer(state, conn)
}

fn handle_list_command(
  state: ConnectionState,
  conn: glisten.Connection(Nil),
  directory: String,
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  let entries =
    process.call(state.vcs, 5000, fn(reply) { HandleList(directory:, reply:) })
  let count = list.length(entries)
  send_response(conn, "OK " <> int.to_string(count) <> "\n")
  list.each(entries, fn(entry) { send_response(conn, entry <> "\n") })
  send_response(conn, "READY\n")
  process_buffer(state, conn)
}

fn process_data_buffer(
  state: ConnectionState,
  conn: glisten.Connection(Nil),
  file: String,
  remaining: Int,
  accumulated: BitArray,
) -> glisten.Next(ConnectionState, glisten.Message(Nil)) {
  let available = bit_array.byte_size(state.buffer)
  case available >= remaining {
    False -> {
      let new_accumulated = bit_array.append(accumulated, state.buffer)
      let new_remaining = remaining - available
      let state =
        ConnectionState(
          ..state,
          buffer: <<>>,
          mode: AwaitingData(
            file:,
            remaining: new_remaining,
            accumulated: new_accumulated,
          ),
        )
      glisten.continue(state)
    }
    True -> {
      let assert Ok(data_part) = take_bytes(state.buffer, remaining)
      let assert Ok(rest) = drop_bytes(state.buffer, remaining)
      let full_data = bit_array.append(accumulated, data_part)

      let result =
        process.call(state.vcs, 5000, fn(reply) {
          HandlePut(file:, data: full_data, reply:)
        })
      case result {
        Error(msg) -> send_response(conn, "ERR " <> msg <> "\n")
        Ok(rev) -> send_response(conn, "OK r" <> int.to_string(rev) <> "\n")
      }
      send_response(conn, "READY\n")

      let state = ConnectionState(..state, buffer: rest, mode: AwaitingCommand)
      process_buffer(state, conn)
    }
  }
}

fn find_newline(data: BitArray) -> Result(#(BitArray, BitArray), Nil) {
  find_newline_at(data, 0)
}

fn find_newline_at(
  data: BitArray,
  offset: Int,
) -> Result(#(BitArray, BitArray), Nil) {
  use <- bool.guard(
    when: offset >= bit_array.byte_size(data),
    return: Error(Nil),
  )

  case bit_array.slice(data, offset, 1) {
    Ok(<<0x0A>>) -> {
      let assert Ok(line) = take_bytes(data, offset)
      let assert Ok(rest) = drop_bytes(data, offset + 1)
      Ok(#(line, rest))
    }
    _ -> find_newline_at(data, offset + 1)
  }
}

fn take_bytes(data: BitArray, count: Int) -> Result(BitArray, Nil) {
  bit_array.slice(data, 0, count)
}

fn drop_bytes(data: BitArray, count: Int) -> Result(BitArray, Nil) {
  let remaining = bit_array.byte_size(data) - count
  bit_array.slice(data, count, remaining)
}

fn send_response(conn: glisten.Connection(Nil), text: String) -> Nil {
  let _ = glisten.send(conn, bytes_tree.from_string(text))
  Nil
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let vcs = start_vcs()
  let assert Ok(_) =
    glisten.new(handle_connection(vcs, _), handle_tcp_message)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
