import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import grammy
import logging
import nibble
import nibble/lexer

pub type LrcpToken {
  Slash
  Content(String)
}

pub type LrcpMessage {
  Connect(session: Int)
  Data(session: Int, pos: Int, data: String)
  Ack(session: Int, length: Int)
  Close(session: Int)
}

type TimerMessage {
  RetransmitTimeout(session_id: Int)
  ExpiryTimeout(session_id: Int)
}

type Session {
  Session(
    address: #(Int, Int, Int, Int),
    port: Int,
    total_received: Int,
    line_buffer: String,
    send_buffer: String,
    total_sent: Int,
    highest_ack: Int,
    retransmit_timer: Option(process.Timer),
    expiry_timer: process.Timer,
  )
}

type ServerState {
  ServerState(sessions: Dict(Int, Session), subject: Subject(TimerMessage))
}

const max_message_size = 1000

const max_numeric_value = 2_147_483_648

const max_chunk_size = 480

const retransmit_timeout_ms = 3000

const expiry_timeout_ms = 60_000

pub fn parse_message(data: BitArray) -> Result(LrcpMessage, Nil) {
  use <- bool.guard(
    when: bit_array.byte_size(data) > max_message_size,
    return: Error(Nil),
  )

  use text <- result.try(bit_array.to_string(data))
  use tokens <- result.try(tokenize(text))
  nibble.run(tokens, lrcp_parser())
  |> result.replace_error(Nil)
}

pub fn serialize_message(msg: LrcpMessage) -> String {
  case msg {
    Connect(session:) -> "/connect/" <> int.to_string(session) <> "/"
    Data(session:, pos:, data:) ->
      "/data/"
      <> int.to_string(session)
      <> "/"
      <> int.to_string(pos)
      <> "/"
      <> data
      <> "/"
    Ack(session:, length:) ->
      "/ack/" <> int.to_string(session) <> "/" <> int.to_string(length) <> "/"
    Close(session:) -> "/close/" <> int.to_string(session) <> "/"
  }
}

fn tokenize(input: String) -> Result(List(lexer.Token(LrcpToken)), Nil) {
  tokenize_loop(string.to_graphemes(input), "", [])
}

fn tokenize_loop(
  chars: List(String),
  acc: String,
  tokens: List(lexer.Token(LrcpToken)),
) -> Result(List(lexer.Token(LrcpToken)), Nil) {
  case chars {
    [] -> Ok(list.reverse(flush_acc(acc, tokens)))
    ["\\", "\\", ..rest] -> tokenize_loop(rest, acc <> "\\\\", tokens)
    ["\\", "/", ..rest] -> tokenize_loop(rest, acc <> "\\/", tokens)
    ["\\", ..] -> Error(Nil)
    ["/", ..rest] ->
      tokenize_loop(rest, "", [make_token(Slash), ..flush_acc(acc, tokens)])
    [c, ..rest] -> tokenize_loop(rest, acc <> c, tokens)
  }
}

fn flush_acc(
  acc: String,
  tokens: List(lexer.Token(LrcpToken)),
) -> List(lexer.Token(LrcpToken)) {
  case acc {
    "" -> tokens
    _ -> [make_token(Content(acc)), ..tokens]
  }
}

fn make_token(value: LrcpToken) -> lexer.Token(LrcpToken) {
  lexer.Token(span: lexer.Span(1, 1, 1, 1), lexeme: "", value: value)
}

fn lrcp_parser() -> nibble.Parser(LrcpMessage, LrcpToken, Nil) {
  use _ <- nibble.do(nibble.token(Slash))
  use msg <- nibble.do(
    nibble.one_of([
      connect_parser(),
      close_parser(),
      ack_parser(),
      data_parser(),
    ]),
  )
  use _ <- nibble.do(nibble.eof())
  nibble.return(msg)
}

fn connect_parser() -> nibble.Parser(LrcpMessage, LrcpToken, Nil) {
  use _ <- nibble.do(nibble.token(Content("connect")))
  use _ <- nibble.do(nibble.token(Slash))
  use session <- nibble.do(number_token())
  use _ <- nibble.do(nibble.token(Slash))
  nibble.return(Connect(session))
}

fn close_parser() -> nibble.Parser(LrcpMessage, LrcpToken, Nil) {
  use _ <- nibble.do(nibble.token(Content("close")))
  use _ <- nibble.do(nibble.token(Slash))
  use session <- nibble.do(number_token())
  use _ <- nibble.do(nibble.token(Slash))
  nibble.return(Close(session))
}

fn ack_parser() -> nibble.Parser(LrcpMessage, LrcpToken, Nil) {
  use _ <- nibble.do(nibble.token(Content("ack")))
  use _ <- nibble.do(nibble.token(Slash))
  use session <- nibble.do(number_token())
  use _ <- nibble.do(nibble.token(Slash))
  use length <- nibble.do(number_token())
  use _ <- nibble.do(nibble.token(Slash))
  nibble.return(Ack(session, length))
}

fn data_parser() -> nibble.Parser(LrcpMessage, LrcpToken, Nil) {
  use _ <- nibble.do(nibble.token(Content("data")))
  use _ <- nibble.do(nibble.token(Slash))
  use session <- nibble.do(number_token())
  use _ <- nibble.do(nibble.token(Slash))
  use pos <- nibble.do(number_token())
  use _ <- nibble.do(nibble.token(Slash))
  use data <- nibble.do(nibble.one_of([content_token(), nibble.return("")]))
  use _ <- nibble.do(nibble.token(Slash))
  nibble.return(Data(session, pos, data))
}

fn number_token() -> nibble.Parser(Int, LrcpToken, Nil) {
  nibble.take_map("number", fn(tok) {
    case tok {
      Content(s) ->
        case int.parse(s) {
          Ok(n) if n >= 0 && n < max_numeric_value -> Some(n)
          _ -> None
        }
      _ -> None
    }
  })
}

fn content_token() -> nibble.Parser(String, LrcpToken, Nil) {
  nibble.take_map("content", fn(tok) {
    case tok {
      Content(s) -> Some(s)
      _ -> None
    }
  })
}

pub fn escape(text: String) -> String {
  text
  |> string.replace("\\", "\\\\")
  |> string.replace("/", "\\/")
}

pub fn unescape(text: String) -> String {
  text
  |> string.replace("\\/", "/")
  |> string.replace("\\\\", "\\")
}

pub fn process_lines(line_buffer: String, new_data: String) -> #(String, String) {
  let combined = line_buffer <> new_data
  case string.split_once(combined, "\n") {
    Error(_) -> #("", combined)
    Ok(_) -> split_and_reverse(combined)
  }
}

fn split_and_reverse(text: String) -> #(String, String) {
  let parts = string.split(text, "\n")
  let assert [remainder, ..complete_reversed] = list.reverse(parts)
  let reversed =
    complete_reversed
    |> list.reverse
    |> list.map(fn(line) { string.reverse(line) <> "\n" })
    |> string.join("")
  #(reversed, remainder)
}

pub fn chunk_for_send(
  session_id: Int,
  start_pos: Int,
  data: String,
) -> List(String) {
  do_chunk(session_id, start_pos, data, [])
  |> list.reverse
}

fn do_chunk(
  session_id: Int,
  pos: Int,
  remaining: String,
  acc: List(String),
) -> List(String) {
  use <- bool.guard(when: string.is_empty(remaining), return: acc)

  let chunk_size = int.min(max_chunk_size, string.length(remaining))
  let chunk = string.slice(remaining, 0, chunk_size)
  let rest = string.slice(remaining, chunk_size, string.length(remaining))
  let msg = serialize_message(Data(session_id, pos, escape(chunk)))

  case string.byte_size(msg) >= max_message_size {
    True -> {
      let smaller_size = chunk_size / 2
      let chunk2 = string.slice(remaining, 0, smaller_size)
      let rest2 =
        string.slice(remaining, smaller_size, string.length(remaining))
      let msg2 = serialize_message(Data(session_id, pos, escape(chunk2)))
      do_chunk(session_id, pos + smaller_size, rest2, [msg2, ..acc])
    }
    False -> do_chunk(session_id, pos + chunk_size, rest, [msg, ..acc])
  }
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let assert Ok(_) =
    grammy.new(handle_connection, handle_client_data)
    |> grammy.port(3050)
    |> grammy.start

  process.sleep_forever()
}

fn handle_connection() -> #(ServerState, Option(process.Selector(TimerMessage))) {
  let subject = process.new_subject()
  let selector =
    process.new_selector()
    |> process.select(subject)
  let state = ServerState(sessions: dict.new(), subject:)
  #(state, Some(selector))
}

fn handle_client_data(
  msg: grammy.Message(TimerMessage),
  conn: grammy.Connection,
  state: ServerState,
) -> grammy.Next(ServerState, TimerMessage) {
  case msg {
    grammy.Packet(address, port, data) ->
      handle_packet(state, conn, address, port, data)
    grammy.User(timer_msg) -> handle_timer(state, conn, timer_msg)
  }
}

fn handle_packet(
  state: ServerState,
  conn: grammy.Connection,
  address: #(Int, Int, Int, Int),
  port: Int,
  data: BitArray,
) -> grammy.Next(ServerState, TimerMessage) {
  case parse_message(data) {
    Error(_) -> grammy.continue(state)
    Ok(Connect(session_id)) ->
      handle_connect(state, conn, address, port, session_id)
    Ok(Data(session_id, pos, escaped_data)) ->
      handle_data_msg(state, conn, session_id, pos, escaped_data)
    Ok(Ack(session_id, length)) ->
      handle_ack_msg(state, conn, session_id, length)
    Ok(Close(session_id)) ->
      handle_close_msg(state, conn, address, port, session_id)
  }
}

fn handle_connect(
  state: ServerState,
  conn: grammy.Connection,
  address: #(Int, Int, Int, Int),
  port: Int,
  session_id: Int,
) -> grammy.Next(ServerState, TimerMessage) {
  send_response(conn, address, port, serialize_message(Ack(session_id, 0)))
  case dict.get(state.sessions, session_id) {
    Ok(_) -> grammy.continue(state)
    Error(_) -> {
      let expiry_timer =
        process.send_after(
          state.subject,
          expiry_timeout_ms,
          ExpiryTimeout(session_id),
        )
      let session =
        Session(
          address:,
          port:,
          total_received: 0,
          line_buffer: "",
          send_buffer: "",
          total_sent: 0,
          highest_ack: 0,
          retransmit_timer: None,
          expiry_timer:,
        )
      let sessions = dict.insert(state.sessions, session_id, session)
      grammy.continue(ServerState(..state, sessions:))
    }
  }
}

fn handle_data_msg(
  state: ServerState,
  conn: grammy.Connection,
  session_id: Int,
  pos: Int,
  escaped_data: String,
) -> grammy.Next(ServerState, TimerMessage) {
  case dict.get(state.sessions, session_id) {
    Error(_) -> grammy.continue(state)
    Ok(session) -> {
      use <- bool.lazy_guard(when: pos != session.total_received, return: fn() {
        send_response(
          conn,
          session.address,
          session.port,
          serialize_message(Ack(session_id, session.total_received)),
        )
        grammy.continue(state)
      })

      accept_data(state, conn, session, session_id, escaped_data)
    }
  }
}

fn accept_data(
  state: ServerState,
  conn: grammy.Connection,
  session: Session,
  session_id: Int,
  escaped_data: String,
) -> grammy.Next(ServerState, TimerMessage) {
  let unescaped = unescape(escaped_data)
  let new_total = session.total_received + string.length(unescaped)

  send_response(
    conn,
    session.address,
    session.port,
    serialize_message(Ack(session_id, new_total)),
  )

  let #(output, new_line_buffer) = process_lines(session.line_buffer, unescaped)

  let session =
    Session(..session, total_received: new_total, line_buffer: new_line_buffer)
  let session = case string.is_empty(output) {
    True -> session
    False -> send_output(session, conn, session_id, output, state.subject)
  }
  let session = refresh_expiry(session, state.subject, session_id)
  let sessions = dict.insert(state.sessions, session_id, session)
  grammy.continue(ServerState(..state, sessions:))
}

fn send_output(
  session: Session,
  conn: grammy.Connection,
  session_id: Int,
  output: String,
  subject: Subject(TimerMessage),
) -> Session {
  let new_send_buffer = session.send_buffer <> output
  let new_total_sent = string.length(new_send_buffer)
  let chunks = chunk_for_send(session_id, session.total_sent, output)
  list.each(chunks, fn(chunk) {
    send_response(conn, session.address, session.port, chunk)
  })
  let session =
    Session(..session, send_buffer: new_send_buffer, total_sent: new_total_sent)
  start_retransmit(session, subject, session_id)
}

fn handle_ack_msg(
  state: ServerState,
  conn: grammy.Connection,
  session_id: Int,
  length: Int,
) -> grammy.Next(ServerState, TimerMessage) {
  case dict.get(state.sessions, session_id) {
    Error(_) -> grammy.continue(state)
    Ok(session) -> {
      use <- bool.guard(
        when: length < session.highest_ack,
        return: grammy.continue(state),
      )
      use <- bool.lazy_guard(when: length > session.total_sent, return: fn() {
        send_response(
          conn,
          session.address,
          session.port,
          serialize_message(Close(session_id)),
        )
        grammy.continue(remove_session(state, session_id))
      })

      let session = Session(..session, highest_ack: length)
      let session = case length == session.total_sent {
        True -> cancel_retransmit(session)
        False -> retransmit_from(session, conn, session_id, state.subject)
      }
      let session = refresh_expiry(session, state.subject, session_id)
      let sessions = dict.insert(state.sessions, session_id, session)
      grammy.continue(ServerState(..state, sessions:))
    }
  }
}

fn handle_close_msg(
  state: ServerState,
  conn: grammy.Connection,
  address: #(Int, Int, Int, Int),
  port: Int,
  session_id: Int,
) -> grammy.Next(ServerState, TimerMessage) {
  let state = remove_session(state, session_id)
  send_response(conn, address, port, serialize_message(Close(session_id)))
  grammy.continue(state)
}

fn handle_timer(
  state: ServerState,
  conn: grammy.Connection,
  msg: TimerMessage,
) -> grammy.Next(ServerState, TimerMessage) {
  case msg {
    RetransmitTimeout(session_id) -> handle_retransmit(state, conn, session_id)
    ExpiryTimeout(session_id) -> handle_expiry(state, session_id)
  }
}

fn handle_retransmit(
  state: ServerState,
  conn: grammy.Connection,
  session_id: Int,
) -> grammy.Next(ServerState, TimerMessage) {
  case dict.get(state.sessions, session_id) {
    Error(_) -> grammy.continue(state)
    Ok(session) -> {
      use <- bool.guard(
        when: session.highest_ack >= session.total_sent,
        return: grammy.continue(state),
      )

      let session = retransmit_from(session, conn, session_id, state.subject)
      let sessions = dict.insert(state.sessions, session_id, session)
      grammy.continue(ServerState(..state, sessions:))
    }
  }
}

fn handle_expiry(
  state: ServerState,
  session_id: Int,
) -> grammy.Next(ServerState, TimerMessage) {
  let state = remove_session(state, session_id)
  grammy.continue(state)
}

fn retransmit_from(
  session: Session,
  conn: grammy.Connection,
  session_id: Int,
  subject: Subject(TimerMessage),
) -> Session {
  let session = cancel_retransmit(session)
  let unsent_data =
    string.slice(
      session.send_buffer,
      session.highest_ack,
      session.total_sent - session.highest_ack,
    )
  let chunks = chunk_for_send(session_id, session.highest_ack, unsent_data)

  list.each(chunks, fn(chunk) {
    send_response(conn, session.address, session.port, chunk)
  })

  start_retransmit(session, subject, session_id)
}

fn refresh_expiry(
  session: Session,
  subject: Subject(TimerMessage),
  session_id: Int,
) -> Session {
  let _ = process.cancel_timer(session.expiry_timer)
  let timer =
    process.send_after(subject, expiry_timeout_ms, ExpiryTimeout(session_id))
  Session(..session, expiry_timer: timer)
}

fn start_retransmit(
  session: Session,
  subject: Subject(TimerMessage),
  session_id: Int,
) -> Session {
  let session = cancel_retransmit(session)
  let timer =
    process.send_after(
      subject,
      retransmit_timeout_ms,
      RetransmitTimeout(session_id),
    )
  Session(..session, retransmit_timer: Some(timer))
}

fn cancel_retransmit(session: Session) -> Session {
  case session.retransmit_timer {
    Some(timer) -> {
      let _ = process.cancel_timer(timer)
      Session(..session, retransmit_timer: None)
    }
    None -> session
  }
}

fn remove_session(state: ServerState, session_id: Int) -> ServerState {
  case dict.get(state.sessions, session_id) {
    Error(_) -> state
    Ok(session) -> {
      let _ = process.cancel_timer(session.expiry_timer)
      let _ = option.map(session.retransmit_timer, process.cancel_timer)
      ServerState(..state, sessions: dict.delete(state.sessions, session_id))
    }
  }
}

fn send_response(
  conn: grammy.Connection,
  address: #(Int, Int, Int, Int),
  port: Int,
  message: String,
) -> Nil {
  let _ = grammy.send_to(conn, address, port, bytes_tree.from_string(message))
  Nil
}
