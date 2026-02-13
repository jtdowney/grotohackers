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
import gleam/result
import gleam/set.{type Set}
import glisten.{Packet, User}

pub type Plate =
  String

pub type Road =
  Int

pub type Mile =
  Int

pub type Timestamp =
  Int

pub type Speed =
  Int

pub type Day =
  Int

pub type ClientMessage {
  Plate(plate: Plate, timestamp: Timestamp)
  WantHeartbeat(interval: Int)
  IAmCamera(road: Road, mile: Mile, limit: Speed)
  IAmDispatcher(roads: List(Road))
}

pub type ServerMessage {
  ErrorMsg(msg: String)
  TicketMsg(ticket: Ticket)
  HeartbeatMsg
}

pub type Ticket {
  Ticket(
    plate: Plate,
    road: Road,
    mile1: Mile,
    timestamp1: Timestamp,
    mile2: Mile,
    timestamp2: Timestamp,
    speed: Speed,
  )
}

pub type ClientRole {
  Unidentified
  Camera(road: Road, mile: Mile, limit: Speed)
  Dispatcher(roads: List(Road))
}

pub type ParseError {
  NeedMoreData
  UnknownMessage(Int)
  InvalidMessage(String)
}

pub type ConnectionState {
  ConnectionState(
    buffer: BitArray,
    role: ClientRole,
    heartbeat_requested: Bool,
    heartbeat_interval_ms: Int,
    server: Subject(ServerCommand),
    send_subject: Subject(ServerMessage),
  )
}

pub type Observation {
  Observation(mile: Mile, timestamp: Timestamp, limit: Speed)
}

pub type ServerCommand {
  RecordObservation(
    plate: Plate,
    road: Road,
    mile: Mile,
    timestamp: Timestamp,
    limit: Speed,
  )
  RegisterDispatcher(roads: List(Road), subject: Subject(ServerMessage))
  UnregisterDispatcher(subject: Subject(ServerMessage))
}

pub type ServerState {
  ServerState(
    observations: Dict(#(Plate, Road), List(Observation)),
    ticketed_days: Dict(Plate, Set(Day)),
    dispatchers: Dict(Road, List(Subject(ServerMessage))),
    pending_tickets: Dict(Road, List(Ticket)),
  )
}

pub fn parse_string(data: BitArray) -> Result(#(String, BitArray), ParseError) {
  case data {
    <<len:8, rest:bytes>> -> {
      use <- bool.guard(
        when: bit_array.byte_size(rest) < len,
        return: Error(NeedMoreData),
      )

      let assert <<str_bytes:bytes-size(len), remaining:bytes>> = rest
      bit_array.to_string(str_bytes)
      |> result.map(fn(s) { #(s, remaining) })
      |> result.replace_error(InvalidMessage("Invalid UTF-8 in string"))
    }
    _ -> Error(NeedMoreData)
  }
}

pub fn parse_message(
  data: BitArray,
) -> Result(#(ClientMessage, BitArray), ParseError) {
  case data {
    <<>> -> Error(NeedMoreData)
    <<0x20, rest:bytes>> -> parse_plate_message(rest)
    <<0x40, rest:bytes>> -> parse_want_heartbeat(rest)
    <<0x80, rest:bytes>> -> parse_i_am_camera(rest)
    <<0x81, rest:bytes>> -> parse_i_am_dispatcher(rest)
    <<tag:8, _:bytes>> -> Error(UnknownMessage(tag))
    _ -> Error(NeedMoreData)
  }
}

fn parse_plate_message(
  data: BitArray,
) -> Result(#(ClientMessage, BitArray), ParseError) {
  use #(plate, rest) <- result.try(parse_string(data))
  case rest {
    <<ts:unsigned-size(32)-big, remaining:bytes>> ->
      Ok(#(Plate(plate:, timestamp: ts), remaining))
    _ -> Error(NeedMoreData)
  }
}

fn parse_want_heartbeat(
  data: BitArray,
) -> Result(#(ClientMessage, BitArray), ParseError) {
  case data {
    <<interval:unsigned-size(32)-big, rest:bytes>> ->
      Ok(#(WantHeartbeat(interval:), rest))
    _ -> Error(NeedMoreData)
  }
}

fn parse_i_am_camera(
  data: BitArray,
) -> Result(#(ClientMessage, BitArray), ParseError) {
  case data {
    <<
      road:unsigned-size(16)-big,
      mile:unsigned-size(16)-big,
      limit:unsigned-size(16)-big,
      rest:bytes,
    >> -> Ok(#(IAmCamera(road:, mile:, limit:), rest))
    _ -> Error(NeedMoreData)
  }
}

fn parse_i_am_dispatcher(
  data: BitArray,
) -> Result(#(ClientMessage, BitArray), ParseError) {
  case data {
    <<num_roads:8, rest:bytes>> -> parse_road_list([], rest, num_roads)
    _ -> Error(NeedMoreData)
  }
}

fn parse_road_list(
  acc: List(Road),
  data: BitArray,
  remaining: Int,
) -> Result(#(ClientMessage, BitArray), ParseError) {
  use <- bool.guard(
    when: remaining == 0,
    return: Ok(#(IAmDispatcher(roads: list.reverse(acc)), data)),
  )

  case data {
    <<road:unsigned-size(16)-big, rest:bytes>> ->
      parse_road_list([road, ..acc], rest, remaining - 1)
    _ -> Error(NeedMoreData)
  }
}

pub fn process_buffer(
  buffer: BitArray,
  new_data: BitArray,
) -> Result(#(List(ClientMessage), BitArray), String) {
  bit_array.append(buffer, new_data)
  |> extract_messages([])
}

fn extract_messages(
  data: BitArray,
  acc: List(ClientMessage),
) -> Result(#(List(ClientMessage), BitArray), String) {
  case parse_message(data) {
    Ok(#(msg, rest)) -> extract_messages(rest, [msg, ..acc])
    Error(NeedMoreData) -> Ok(#(list.reverse(acc), data))
    Error(UnknownMessage(tag)) ->
      Error("Unknown message type: " <> int.to_string(tag))
    Error(InvalidMessage(reason)) -> Error(reason)
  }
}

pub fn encode_message(msg: ServerMessage) -> BitArray {
  case msg {
    ErrorMsg(text) -> {
      let text_bytes = bit_array.from_string(text)
      let len = bit_array.byte_size(text_bytes)
      <<0x10, len:8, text_bytes:bits>>
    }
    TicketMsg(t) -> {
      let plate_bytes = bit_array.from_string(t.plate)
      let plate_len = bit_array.byte_size(plate_bytes)
      <<
        0x21,
        plate_len:8,
        plate_bytes:bits,
        t.road:size(16)-big,
        t.mile1:size(16)-big,
        t.timestamp1:size(32)-big,
        t.mile2:size(16)-big,
        t.timestamp2:size(32)-big,
        t.speed:size(16)-big,
      >>
    }
    HeartbeatMsg -> <<0x41>>
  }
}

pub fn calculate_speed(
  mile1: Mile,
  timestamp1: Timestamp,
  mile2: Mile,
  timestamp2: Timestamp,
) -> Speed {
  let distance = int.absolute_value(mile2 - mile1)
  case int.absolute_value(timestamp2 - timestamp1) {
    0 -> 0
    time -> distance * 360_000 / time
  }
}

pub fn days_covered(timestamp1: Timestamp, timestamp2: Timestamp) -> Set(Day) {
  let low = int.min(timestamp1, timestamp2) / 86_400
  let high = int.max(timestamp1, timestamp2) / 86_400
  int.range(from: low, to: high + 1, with: set.new(), run: set.insert)
}

pub fn start_server() -> Subject(ServerCommand) {
  let initial_state =
    ServerState(
      observations: dict.new(),
      ticketed_days: dict.new(),
      dispatchers: dict.new(),
      pending_tickets: dict.new(),
    )

  let assert Ok(started) =
    actor.new(initial_state)
    |> actor.on_message(handle_server_message)
    |> actor.start

  started.data
}

fn handle_server_message(
  state: ServerState,
  msg: ServerCommand,
) -> actor.Next(ServerState, ServerCommand) {
  case msg {
    RecordObservation(plate:, road:, mile:, timestamp:, limit:) ->
      handle_record_observation(state, plate, road, mile, timestamp, limit)
    RegisterDispatcher(roads:, subject:) ->
      handle_register_dispatcher(state, roads, subject)
    UnregisterDispatcher(subject:) ->
      handle_unregister_dispatcher(state, subject)
  }
}

fn handle_record_observation(
  state: ServerState,
  plate: Plate,
  road: Road,
  mile: Mile,
  timestamp: Timestamp,
  limit: Speed,
) -> actor.Next(ServerState, ServerCommand) {
  let key = #(plate, road)
  let obs = Observation(mile:, timestamp:, limit:)
  let existing = dict.get(state.observations, key) |> result.unwrap([])
  let new_observations = dict.insert(state.observations, key, [obs, ..existing])
  let tickets = generate_tickets(plate, road, obs, existing)

  let #(new_ticketed_days, new_pending) =
    list.fold(
      tickets,
      #(state.ticketed_days, state.pending_tickets),
      fn(acc, ticket) { process_ticket(acc, plate, state.dispatchers, ticket) },
    )

  actor.continue(
    ServerState(
      ..state,
      observations: new_observations,
      ticketed_days: new_ticketed_days,
      pending_tickets: new_pending,
    ),
  )
}

fn generate_tickets(
  plate: Plate,
  road: Road,
  new_obs: Observation,
  existing: List(Observation),
) -> List(Ticket) {
  list.filter_map(existing, fn(old_obs) {
    let speed =
      calculate_speed(
        old_obs.mile,
        old_obs.timestamp,
        new_obs.mile,
        new_obs.timestamp,
      )
    let limit = int.max(old_obs.limit, new_obs.limit)
    use <- bool.guard(when: speed < limit * 100 + 50, return: Error(Nil))

    let #(first, second) = case old_obs.timestamp <= new_obs.timestamp {
      True -> #(old_obs, new_obs)
      False -> #(new_obs, old_obs)
    }
    Ok(Ticket(
      plate:,
      road:,
      mile1: first.mile,
      timestamp1: first.timestamp,
      mile2: second.mile,
      timestamp2: second.timestamp,
      speed:,
    ))
  })
}

fn process_ticket(
  acc: #(Dict(Plate, Set(Day)), Dict(Road, List(Ticket))),
  plate: Plate,
  dispatchers: Dict(Road, List(Subject(ServerMessage))),
  ticket: Ticket,
) -> #(Dict(Plate, Set(Day)), Dict(Road, List(Ticket))) {
  let #(ticketed_days, pending) = acc
  let covered = days_covered(ticket.timestamp1, ticket.timestamp2)
  let already_ticketed_days =
    dict.get(ticketed_days, plate) |> result.unwrap(set.new())

  use <- bool.guard(
    when: !set.is_disjoint(already_ticketed_days, covered),
    return: #(ticketed_days, pending),
  )

  let new_days = set.union(already_ticketed_days, covered)
  let new_ticketed = dict.insert(ticketed_days, plate, new_days)
  let new_pending = try_dispatch_ticket(pending, dispatchers, ticket)
  #(new_ticketed, new_pending)
}

fn try_dispatch_ticket(
  pending: Dict(Road, List(Ticket)),
  dispatchers: Dict(Road, List(Subject(ServerMessage))),
  ticket: Ticket,
) -> Dict(Road, List(Ticket)) {
  case dict.get(dispatchers, ticket.road) {
    Ok([dispatcher, ..]) -> {
      process.send(dispatcher, TicketMsg(ticket))
      pending
    }
    _ -> {
      let existing = dict.get(pending, ticket.road) |> result.unwrap([])
      dict.insert(pending, ticket.road, [ticket, ..existing])
    }
  }
}

fn handle_register_dispatcher(
  state: ServerState,
  roads: List(Road),
  subject: Subject(ServerMessage),
) -> actor.Next(ServerState, ServerCommand) {
  let new_dispatchers =
    list.fold(roads, state.dispatchers, fn(dispatchers, road) {
      dict.upsert(in: dispatchers, update: road, with: fn(existing) {
        case existing {
          Some(existing) -> [subject, ..existing]
          None -> [subject]
        }
      })
    })

  let new_pending =
    list.fold(roads, state.pending_tickets, fn(pending, road) {
      case dict.get(pending, road) {
        Ok(tickets) -> {
          list.each(tickets, fn(ticket) {
            process.send(subject, TicketMsg(ticket))
          })

          dict.delete(pending, road)
        }
        Error(_) -> pending
      }
    })

  actor.continue(
    ServerState(
      ..state,
      dispatchers: new_dispatchers,
      pending_tickets: new_pending,
    ),
  )
}

fn handle_unregister_dispatcher(
  state: ServerState,
  subject: Subject(ServerMessage),
) -> actor.Next(ServerState, ServerCommand) {
  let new_dispatchers =
    dict.map_values(state.dispatchers, fn(_, subjects) {
      list.filter(subjects, fn(s) { s != subject })
    })
  actor.continue(ServerState(..state, dispatchers: new_dispatchers))
}

fn handle_connection(
  server: Subject(ServerCommand),
  conn: glisten.Connection(ServerMessage),
) -> #(ConnectionState, Option(process.Selector(ServerMessage))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  io.println(
    "New connection from "
    <> glisten.ip_address_to_string(ip_address)
    <> " on "
    <> int.to_string(port),
  )

  let send_subject = process.new_subject()
  let selector =
    process.new_selector()
    |> process.select(send_subject)

  let state =
    ConnectionState(
      buffer: <<>>,
      role: Unidentified,
      heartbeat_requested: False,
      heartbeat_interval_ms: 0,
      server:,
      send_subject:,
    )
  #(state, Some(selector))
}

fn handle_client_data(
  state: ConnectionState,
  msg: glisten.Message(ServerMessage),
  conn: glisten.Connection(ServerMessage),
) -> glisten.Next(ConnectionState, glisten.Message(ServerMessage)) {
  case msg {
    Packet(data) -> handle_packet(state, data, conn)
    User(HeartbeatMsg) -> {
      let _ = glisten.send(conn, bytes_tree.from_bit_array(<<0x41>>))
      process.send_after(
        state.send_subject,
        state.heartbeat_interval_ms,
        HeartbeatMsg,
      )
      glisten.continue(state)
    }
    User(TicketMsg(ticket)) -> {
      let encoded = encode_message(TicketMsg(ticket))
      let _ = glisten.send(conn, bytes_tree.from_bit_array(encoded))
      glisten.continue(state)
    }
    User(ErrorMsg(_)) -> glisten.continue(state)
  }
}

fn handle_packet(
  state: ConnectionState,
  data: BitArray,
  conn: glisten.Connection(ServerMessage),
) -> glisten.Next(ConnectionState, glisten.Message(ServerMessage)) {
  case process_buffer(state.buffer, data) {
    Error(reason) -> send_error_and_disconnect(conn, reason)
    Ok(#(messages, new_buffer)) -> {
      let new_state = ConnectionState(..state, buffer: new_buffer)
      process_client_messages(new_state, messages, conn)
    }
  }
}

fn process_client_messages(
  state: ConnectionState,
  messages: List(ClientMessage),
  conn: glisten.Connection(ServerMessage),
) -> glisten.Next(ConnectionState, glisten.Message(ServerMessage)) {
  case messages {
    [] -> glisten.continue(state)
    [msg, ..rest] ->
      case handle_message(state, msg) {
        Ok(new_state) -> process_client_messages(new_state, rest, conn)
        Error(reason) -> send_error_and_disconnect(conn, reason)
      }
  }
}

fn handle_message(
  state: ConnectionState,
  msg: ClientMessage,
) -> Result(ConnectionState, String) {
  case msg {
    IAmCamera(road:, mile:, limit:) -> {
      use <- guard_role(state, Unidentified)
      Ok(ConnectionState(..state, role: Camera(road:, mile:, limit:)))
    }
    IAmDispatcher(roads:) -> {
      use <- guard_role(state, Unidentified)
      process.send(
        state.server,
        RegisterDispatcher(roads:, subject: state.send_subject),
      )
      Ok(ConnectionState(..state, role: Dispatcher(roads:)))
    }
    Plate(plate:, timestamp:) -> {
      case state.role {
        Camera(road:, mile:, limit:) -> {
          process.send(
            state.server,
            RecordObservation(plate:, road:, mile:, timestamp:, limit:),
          )
          Ok(state)
        }
        _ -> Error("Plate message from non-camera client")
      }
    }
    WantHeartbeat(interval:) -> {
      use <- bool.guard(
        when: state.heartbeat_requested,
        return: Error("Heartbeat already requested"),
      )
      let interval_ms = interval * 100
      case interval > 0 {
        True -> {
          process.send_after(state.send_subject, interval_ms, HeartbeatMsg)
          Nil
        }
        False -> Nil
      }
      Ok(
        ConnectionState(
          ..state,
          heartbeat_requested: True,
          heartbeat_interval_ms: interval_ms,
        ),
      )
    }
  }
}

fn guard_role(
  state: ConnectionState,
  expected: ClientRole,
  continue: fn() -> Result(ConnectionState, String),
) -> Result(ConnectionState, String) {
  case state.role == expected {
    True -> continue()
    False -> Error("Client already identified")
  }
}

fn send_error_and_disconnect(
  conn: glisten.Connection(ServerMessage),
  reason: String,
) -> glisten.Next(ConnectionState, glisten.Message(ServerMessage)) {
  let encoded = encode_message(ErrorMsg(reason))
  let _ = glisten.send(conn, bytes_tree.from_bit_array(encoded))
  glisten.stop()
}

fn handle_close(state: ConnectionState) -> Nil {
  case state.role {
    Dispatcher(_) ->
      process.send(state.server, UnregisterDispatcher(state.send_subject))
    _ -> Nil
  }
}

pub fn main() -> Nil {
  let server = start_server()
  let assert Ok(_) =
    glisten.new(handle_connection(server, _), handle_client_data)
    |> glisten.with_close(handle_close)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
