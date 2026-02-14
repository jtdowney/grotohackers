import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{Packet, User}
import logging
import mug

pub type Message {
  Hello(protocol: String, version: Int)
  MsgError(message: String)
  MsgOk
  DialAuthority(site: Int)
  TargetPopulations(site: Int, populations: List(PopulationTarget))
  CreatePolicy(species: String, action: PolicyAction)
  DeletePolicy(policy: Int)
  PolicyResult(policy: Int)
  SiteVisit(site: Int, populations: List(PopulationObservation))
}

pub type PolicyAction {
  Cull
  Conserve
}

pub type PopulationTarget {
  PopulationTarget(species: String, min: Int, max: Int)
}

pub type PopulationObservation {
  PopulationObservation(species: String, count: Int)
}

pub type ParseError {
  NeedMoreData
  BadChecksum
  UnknownMessageType(Int)
  InvalidMessage(String)
}

pub type PolicyChange {
  CreateNewPolicy(species: String, action: PolicyAction)
  DeleteExistingPolicy(species: String, policy_id: Int)
}

pub fn compute_checksum(data: BitArray) -> Int {
  let sum = sum_bytes(data, 0)
  { 256 - sum % 256 } % 256
}

pub fn verify_checksum(data: BitArray) -> Bool {
  sum_bytes(data, 0) % 256 == 0
}

fn sum_bytes(data: BitArray, acc: Int) -> Int {
  case data {
    <<byte:8, rest:bytes>> -> sum_bytes(rest, acc + byte)
    _ -> acc
  }
}

pub fn parse_str(data: BitArray) -> Result(#(String, BitArray), ParseError) {
  use #(len, rest) <- result.try(parse_u32(data))
  use <- bool.guard(
    when: bit_array.byte_size(rest) < len,
    return: Error(NeedMoreData),
  )

  let assert <<str_bytes:bytes-size(len), remaining:bytes>> = rest
  bit_array.to_string(str_bytes)
  |> result.map(fn(s) { #(s, remaining) })
  |> result.replace_error(InvalidMessage("Invalid UTF-8 in string"))
}

fn parse_u32(data: BitArray) -> Result(#(Int, BitArray), ParseError) {
  case data {
    <<value:unsigned-size(32)-big, rest:bytes>> -> Ok(#(value, rest))
    _ -> Error(NeedMoreData)
  }
}

fn parse_array(
  data: BitArray,
  parser_fn: fn(BitArray) -> Result(#(a, BitArray), ParseError),
) -> Result(#(List(a), BitArray), ParseError) {
  use #(count, rest) <- result.try(parse_u32(data))
  parse_array_elements(rest, parser_fn, count, [])
}

fn parse_array_elements(
  data: BitArray,
  parser_fn: fn(BitArray) -> Result(#(a, BitArray), ParseError),
  remaining: Int,
  acc: List(a),
) -> Result(#(List(a), BitArray), ParseError) {
  use <- bool.guard(
    when: remaining == 0,
    return: Ok(#(list.reverse(acc), data)),
  )

  use #(item, rest) <- result.try(parser_fn(data))
  parse_array_elements(rest, parser_fn, remaining - 1, [item, ..acc])
}

const max_message_length = 1_000_000

pub fn parse_message(data: BitArray) -> Result(#(Message, BitArray), ParseError) {
  use <- bool.guard(
    when: bit_array.byte_size(data) < 5,
    return: Error(NeedMoreData),
  )

  let assert <<_type_byte:8, length:unsigned-size(32)-big, _:bytes>> = data
  use <- bool.guard(
    when: length < 6,
    return: Error(InvalidMessage("Message length too short")),
  )
  use <- bool.guard(
    when: length > max_message_length,
    return: Error(InvalidMessage("Message length too large")),
  )
  use <- bool.guard(
    when: bit_array.byte_size(data) < length,
    return: Error(NeedMoreData),
  )

  let assert <<frame:bytes-size(length), remaining:bytes>> = data
  use <- bool.guard(when: !verify_checksum(frame), return: Error(BadChecksum))

  let content_size = length - 6
  let assert <<
    type_byte:8,
    _length:unsigned-size(32)-big,
    content:bytes-size(content_size),
    _checksum:8,
  >> = frame

  case type_byte {
    0x50 -> parse_hello(content)
    0x51 -> parse_error(content)
    0x52 -> parse_ok(content)
    0x53 -> parse_dial_authority(content)
    0x54 -> parse_target_populations(content)
    0x55 -> parse_create_policy(content)
    0x56 -> parse_delete_policy(content)
    0x57 -> parse_policy_result(content)
    0x58 -> parse_site_visit(content)
    other -> Error(UnknownMessageType(other))
  }
  |> result.map_error(fn(e) {
    case e {
      NeedMoreData -> InvalidMessage("Truncated message content")
      other -> other
    }
  })
  |> result.map(fn(msg) { #(msg, remaining) })
}

fn parse_hello(content: BitArray) -> Result(Message, ParseError) {
  use #(protocol, rest) <- result.try(parse_str(content))
  case rest {
    <<version:unsigned-size(32)-big>> -> Ok(Hello(protocol:, version:))
    _ -> Error(InvalidMessage("Invalid Hello message"))
  }
}

fn parse_error(content: BitArray) -> Result(Message, ParseError) {
  use #(message, rest) <- result.try(parse_str(content))
  use <- bool.guard(
    when: bit_array.byte_size(rest) != 0,
    return: Error(InvalidMessage("Trailing data in Error message")),
  )
  Ok(MsgError(message:))
}

fn parse_ok(content: BitArray) -> Result(Message, ParseError) {
  use <- bool.guard(
    when: bit_array.byte_size(content) != 0,
    return: Error(InvalidMessage("OK message should have no content")),
  )
  Ok(MsgOk)
}

fn parse_dial_authority(content: BitArray) -> Result(Message, ParseError) {
  case content {
    <<site:unsigned-size(32)-big>> -> Ok(DialAuthority(site:))
    _ -> Error(InvalidMessage("Invalid DialAuthority message"))
  }
}

fn parse_target_populations(content: BitArray) -> Result(Message, ParseError) {
  use #(site, rest) <- result.try(parse_u32(content))
  use #(populations, _) <- result.try(parse_array(rest, parse_population_target))
  Ok(TargetPopulations(site:, populations:))
}

fn parse_population_target(
  data: BitArray,
) -> Result(#(PopulationTarget, BitArray), ParseError) {
  use #(species, rest) <- result.try(parse_str(data))
  case rest {
    <<min:unsigned-size(32)-big, max:unsigned-size(32)-big, remaining:bytes>> ->
      Ok(#(PopulationTarget(species:, min:, max:), remaining))
    _ -> Error(NeedMoreData)
  }
}

fn parse_create_policy(content: BitArray) -> Result(Message, ParseError) {
  use #(species, rest) <- result.try(parse_str(content))
  case rest {
    <<action_byte:8>> ->
      case action_byte {
        0x90 -> Ok(CreatePolicy(species:, action: Cull))
        0xA0 -> Ok(CreatePolicy(species:, action: Conserve))
        _ -> Error(InvalidMessage("Invalid policy action"))
      }
    _ -> Error(InvalidMessage("Invalid CreatePolicy message"))
  }
}

fn parse_delete_policy(content: BitArray) -> Result(Message, ParseError) {
  case content {
    <<policy:unsigned-size(32)-big>> -> Ok(DeletePolicy(policy:))
    _ -> Error(InvalidMessage("Invalid DeletePolicy message"))
  }
}

fn parse_policy_result(content: BitArray) -> Result(Message, ParseError) {
  case content {
    <<policy:unsigned-size(32)-big>> -> Ok(PolicyResult(policy:))
    _ -> Error(InvalidMessage("Invalid PolicyResult message"))
  }
}

fn parse_site_visit(content: BitArray) -> Result(Message, ParseError) {
  use #(site, rest) <- result.try(parse_u32(content))
  use #(populations, _) <- result.try(parse_array(
    rest,
    parse_population_observation,
  ))
  Ok(SiteVisit(site:, populations:))
}

fn parse_population_observation(
  data: BitArray,
) -> Result(#(PopulationObservation, BitArray), ParseError) {
  use #(species, rest) <- result.try(parse_str(data))
  case rest {
    <<count:unsigned-size(32)-big, remaining:bytes>> ->
      Ok(#(PopulationObservation(species:, count:), remaining))
    _ -> Error(NeedMoreData)
  }
}

pub fn encode_message(msg: Message) -> BitArray {
  let #(type_byte, content) = case msg {
    Hello(protocol:, version:) -> #(
      0x50,
      encode_hello_content(protocol, version),
    )
    MsgError(message:) -> #(0x51, encode_str(message))
    MsgOk -> #(0x52, <<>>)
    DialAuthority(site:) -> #(0x53, <<site:size(32)-big>>)
    TargetPopulations(_, _) -> #(0x54, <<>>)
    CreatePolicy(species:, action:) -> #(
      0x55,
      encode_create_policy_content(species, action),
    )
    DeletePolicy(policy:) -> #(0x56, <<policy:size(32)-big>>)
    PolicyResult(policy:) -> #(0x57, <<policy:size(32)-big>>)
    SiteVisit(_, _) -> #(0x58, <<>>)
  }

  let content_size = bit_array.byte_size(content)
  let length = 1 + 4 + content_size + 1
  let without_checksum = <<
    type_byte:8,
    length:size(32)-big,
    content:bits,
  >>
  let checksum = compute_checksum(without_checksum)
  <<without_checksum:bits, checksum:8>>
}

fn encode_hello_content(protocol: String, version: Int) -> BitArray {
  let str_bytes = encode_str(protocol)
  <<str_bytes:bits, version:size(32)-big>>
}

fn encode_str(s: String) -> BitArray {
  let bytes = bit_array.from_string(s)
  let len = bit_array.byte_size(bytes)
  <<len:size(32)-big, bytes:bits>>
}

fn encode_create_policy_content(
  species: String,
  action: PolicyAction,
) -> BitArray {
  let str_bytes = encode_str(species)
  let action_byte = case action {
    Cull -> 0x90
    Conserve -> 0xA0
  }
  <<str_bytes:bits, action_byte:8>>
}

pub fn process_buffer(
  buffer: BitArray,
  new_data: BitArray,
) -> Result(#(List(Message), BitArray), String) {
  bit_array.append(buffer, new_data)
  |> extract_messages([])
}

fn extract_messages(
  data: BitArray,
  acc: List(Message),
) -> Result(#(List(Message), BitArray), String) {
  case parse_message(data) {
    Ok(#(msg, rest)) -> extract_messages(rest, [msg, ..acc])
    Error(NeedMoreData) -> Ok(#(list.reverse(acc), data))
    Error(BadChecksum) -> Error("Bad checksum")
    Error(UnknownMessageType(tag)) ->
      Error("Unknown message type: " <> int.to_string(tag))
    Error(InvalidMessage(reason)) -> Error(reason)
  }
}

pub fn validate_populations(
  populations: List(PopulationObservation),
) -> Result(Dict(String, Int), String) {
  list.try_fold(populations, dict.new(), fn(acc, obs) {
    case dict.get(acc, obs.species) {
      Error(Nil) -> Ok(dict.insert(acc, obs.species, obs.count))
      Ok(existing_count) if existing_count == obs.count -> Ok(acc)
      Ok(_) -> Error("Conflicting counts for species: " <> obs.species)
    }
  })
}

pub fn compute_policy_changes(
  targets: Dict(String, #(Int, Int)),
  observations: Dict(String, Int),
  current_policies: Dict(String, #(Int, PolicyAction)),
) -> List(PolicyChange) {
  dict.fold(targets, [], fn(changes, species, target) {
    let #(min, max) = target
    let count = dict.get(observations, species) |> result.unwrap(0)
    let needed_action = case count < min, count > max {
      True, _ -> Some(Conserve)
      _, True -> Some(Cull)
      _, _ -> None
    }

    let current = dict.get(current_policies, species)

    case needed_action, current {
      None, Error(Nil) -> changes
      None, Ok(#(policy_id, _)) -> [
        DeleteExistingPolicy(species:, policy_id:),
        ..changes
      ]
      Some(action), Error(Nil) -> [
        CreateNewPolicy(species:, action:),
        ..changes
      ]
      Some(action), Ok(#(_, current_action)) if action == current_action ->
        changes
      Some(action), Ok(#(policy_id, _)) -> [
        CreateNewPolicy(species:, action:),
        DeleteExistingPolicy(species:, policy_id:),
        ..changes
      ]
    }
  })
}

fn connect_authority(
  site: Int,
) -> Result(#(mug.Socket, BitArray, Dict(String, #(Int, Int))), String) {
  use socket <- result.try(
    mug.new("pestcontrol.protohackers.com", port: 20_547)
    |> mug.timeout(milliseconds: 5000)
    |> mug.connect
    |> result.map_error(fn(_) { "Failed to connect to authority" }),
  )

  let hello = encode_message(Hello(protocol: "pestcontrol", version: 1))
  use _ <- result.try(
    mug.send(socket, hello)
    |> result.map_error(fn(e) {
      "Failed to send Hello: " <> mug_error_to_string(e)
    }),
  )

  use #(hello_msg, buffer) <- result.try(
    receive_authority_message(socket, <<>>),
  )
  use _ <- result.try(case hello_msg {
    Hello(protocol: "pestcontrol", version: 1) -> Ok(Nil)
    _ -> Error("Unexpected Hello response from authority")
  })

  let dial = encode_message(DialAuthority(site:))
  use _ <- result.try(
    mug.send(socket, dial)
    |> result.map_error(fn(e) {
      "Failed to send DialAuthority: " <> mug_error_to_string(e)
    }),
  )

  use #(target_msg, buffer2) <- result.try(receive_authority_message(
    socket,
    buffer,
  ))
  case target_msg {
    TargetPopulations(site: recv_site, populations:) if recv_site == site -> {
      let targets =
        list.fold(populations, dict.new(), fn(acc, pop) {
          dict.insert(acc, pop.species, #(pop.min, pop.max))
        })
      Ok(#(socket, buffer2, targets))
    }
    other -> {
      logging.log(
        logging.Warning,
        "Unexpected response to DialAuthority: " <> string.inspect(other),
      )
      Error("Unexpected response to DialAuthority")
    }
  }
}

fn send_authority_message(
  socket: mug.Socket,
  msg: Message,
) -> Result(Nil, String) {
  let encoded = encode_message(msg)
  mug.send(socket, encoded)
  |> result.map_error(fn(e) {
    "Failed to send to authority: " <> mug_error_to_string(e)
  })
}

fn receive_authority_message(
  socket: mug.Socket,
  buffer: BitArray,
) -> Result(#(Message, BitArray), String) {
  case parse_message(buffer) {
    Ok(result) -> Ok(result)
    Error(NeedMoreData) -> {
      case mug.receive(socket, timeout_milliseconds: 5000) {
        Ok(data) ->
          receive_authority_message(socket, bit_array.append(buffer, data))
        Error(e) ->
          Error("Failed to receive from authority: " <> mug_error_to_string(e))
      }
    }
    Error(BadChecksum) -> Error("Bad checksum from authority")
    Error(UnknownMessageType(tag)) ->
      Error("Unknown message type from authority: " <> int.to_string(tag))
    Error(InvalidMessage(reason)) ->
      Error("Invalid message from authority: " <> reason)
  }
}

fn mug_error_to_string(err: mug.Error) -> String {
  case err {
    mug.Timeout -> "timeout"
    mug.Closed -> "closed"
    _ -> "network error"
  }
}

type SiteActorState {
  SiteActorState(
    site: Int,
    authority_socket: mug.Socket,
    authority_buffer: BitArray,
    targets: Dict(String, #(Int, Int)),
    policies: Dict(String, #(Int, PolicyAction)),
  )
}

pub type SiteActorMessage {
  UpdateObservations(observations: Dict(String, Int))
}

fn start_site_actor(site: Int) -> Result(Subject(SiteActorMessage), String) {
  use #(socket, buffer, targets) <- result.try(connect_authority(site))
  logging.log(
    logging.Debug,
    "Connected to authority for site " <> int.to_string(site),
  )

  let state =
    SiteActorState(
      site:,
      authority_socket: socket,
      authority_buffer: buffer,
      targets:,
      policies: dict.new(),
    )

  actor.new(state)
  |> actor.on_message(handle_site_message)
  |> actor.start
  |> result.map(fn(started) { started.data })
  |> result.map_error(fn(_) { "Failed to start site actor" })
}

fn handle_site_message(
  state: SiteActorState,
  msg: SiteActorMessage,
) -> actor.Next(SiteActorState, SiteActorMessage) {
  case msg {
    UpdateObservations(observations:) ->
      handle_update_observations(state, observations)
  }
}

fn handle_update_observations(
  state: SiteActorState,
  observations: Dict(String, Int),
) -> actor.Next(SiteActorState, SiteActorMessage) {
  let changes =
    compute_policy_changes(state.targets, observations, state.policies)

  case execute_policy_changes(state, changes) {
    Ok(new_state) -> actor.continue(new_state)
    Error(reason) -> {
      logging.log(
        logging.Error,
        "Failed to execute policy changes for site "
          <> int.to_string(state.site)
          <> ": "
          <> reason,
      )
      actor.continue(state)
    }
  }
}

fn execute_policy_changes(
  state: SiteActorState,
  changes: List(PolicyChange),
) -> Result(SiteActorState, String) {
  let deletes =
    list.filter_map(changes, fn(c) {
      case c {
        DeleteExistingPolicy(species:, policy_id:) -> Ok(#(species, policy_id))
        _ -> Error(Nil)
      }
    })

  let creates =
    list.filter_map(changes, fn(c) {
      case c {
        CreateNewPolicy(species:, action:) -> Ok(#(species, action))
        _ -> Error(Nil)
      }
    })

  use state <- result.try(
    list.try_fold(deletes, state, fn(state, delete) {
      let #(species, policy_id) = delete
      logging.log(
        logging.Debug,
        "Deleting policy "
          <> int.to_string(policy_id)
          <> " for "
          <> species
          <> " on site "
          <> int.to_string(state.site),
      )
      use _ <- result.try(send_authority_message(
        state.authority_socket,
        DeletePolicy(policy: policy_id),
      ))
      use #(response, new_buffer) <- result.try(receive_authority_message(
        state.authority_socket,
        state.authority_buffer,
      ))
      case response {
        MsgOk -> {
          let new_policies = dict.delete(state.policies, species)
          Ok(
            SiteActorState(
              ..state,
              authority_buffer: new_buffer,
              policies: new_policies,
            ),
          )
        }
        _ -> Error("Unexpected response to DeletePolicy")
      }
    }),
  )

  list.try_fold(creates, state, fn(state, create) {
    let #(species, action) = create
    logging.log(
      logging.Debug,
      "Creating "
        <> policy_action_to_string(action)
        <> " policy for "
        <> species
        <> " on site "
        <> int.to_string(state.site),
    )
    use _ <- result.try(send_authority_message(
      state.authority_socket,
      CreatePolicy(species:, action:),
    ))
    use #(response, new_buffer) <- result.try(receive_authority_message(
      state.authority_socket,
      state.authority_buffer,
    ))
    case response {
      PolicyResult(policy:) -> {
        let new_policies =
          dict.insert(state.policies, species, #(policy, action))
        Ok(
          SiteActorState(
            ..state,
            authority_buffer: new_buffer,
            policies: new_policies,
          ),
        )
      }
      _ -> Error("Unexpected response to CreatePolicy")
    }
  })
}

fn policy_action_to_string(action: PolicyAction) -> String {
  case action {
    Cull -> "cull"
    Conserve -> "conserve"
  }
}

pub type SiteManagerMessage {
  HandleSiteVisit(site: Int, populations: Dict(String, Int))
}

type SiteManagerState {
  SiteManagerState(sites: Dict(Int, Subject(SiteActorMessage)))
}

fn start_site_manager() -> Subject(SiteManagerMessage) {
  let assert Ok(started) =
    actor.new(SiteManagerState(sites: dict.new()))
    |> actor.on_message(handle_site_manager_message)
    |> actor.start

  started.data
}

fn handle_site_manager_message(
  state: SiteManagerState,
  msg: SiteManagerMessage,
) -> actor.Next(SiteManagerState, SiteManagerMessage) {
  case msg {
    HandleSiteVisit(site:, populations:) -> {
      let #(new_state, site_subject) = get_or_create_site(state, site)
      case site_subject {
        Some(subject) ->
          process.send(subject, UpdateObservations(observations: populations))
        None -> Nil
      }
      actor.continue(new_state)
    }
  }
}

fn get_or_create_site(
  state: SiteManagerState,
  site: Int,
) -> #(SiteManagerState, Option(Subject(SiteActorMessage))) {
  case dict.get(state.sites, site) {
    Ok(subject) -> #(state, Some(subject))
    Error(Nil) -> {
      case start_site_actor(site) {
        Ok(subject) -> {
          let new_sites = dict.insert(state.sites, site, subject)
          #(SiteManagerState(sites: new_sites), Some(subject))
        }
        Error(reason) -> {
          logging.log(
            logging.Error,
            "Failed to start site actor for site "
              <> int.to_string(site)
              <> ": "
              <> reason,
          )
          #(state, None)
        }
      }
    }
  }
}

pub type ClientState {
  ClientState(
    buffer: BitArray,
    site_manager: Subject(SiteManagerMessage),
    hello_received: Bool,
  )
}

fn handle_connection(
  site_manager: Subject(SiteManagerMessage),
  conn: glisten.Connection(Nil),
) -> #(ClientState, Option(process.Selector(Nil))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  logging.log(
    logging.Debug,
    "New client from "
      <> glisten.ip_address_to_string(ip_address)
      <> " on "
      <> int.to_string(port),
  )

  let hello = encode_message(Hello(protocol: "pestcontrol", version: 1))
  let _ = glisten.send(conn, bytes_tree.from_bit_array(hello))

  #(ClientState(buffer: <<>>, site_manager:, hello_received: False), None)
}

fn handle_client_data(
  state: ClientState,
  msg: glisten.Message(Nil),
  conn: glisten.Connection(Nil),
) -> glisten.Next(ClientState, glisten.Message(Nil)) {
  case msg {
    Packet(data) -> handle_client_packet(state, data, conn)
    User(_) -> glisten.continue(state)
  }
}

fn handle_client_packet(
  state: ClientState,
  data: BitArray,
  conn: glisten.Connection(Nil),
) -> glisten.Next(ClientState, glisten.Message(Nil)) {
  case process_buffer(state.buffer, data) {
    Error(reason) -> send_error_and_disconnect(conn, reason)
    Ok(#(messages, new_buffer)) -> {
      let new_state = ClientState(..state, buffer: new_buffer)
      process_client_messages(new_state, messages, conn, state.site_manager)
    }
  }
}

fn process_client_messages(
  state: ClientState,
  messages: List(Message),
  conn: glisten.Connection(Nil),
  site_manager: Subject(SiteManagerMessage),
) -> glisten.Next(ClientState, glisten.Message(Nil)) {
  case messages {
    [] -> glisten.continue(state)
    [msg, ..rest] ->
      case handle_client_message(state, msg, site_manager) {
        Ok(new_state) ->
          process_client_messages(new_state, rest, conn, site_manager)
        Error(reason) -> send_error_and_disconnect(conn, reason)
      }
  }
}

fn handle_client_message(
  state: ClientState,
  msg: Message,
  site_manager: Subject(SiteManagerMessage),
) -> Result(ClientState, String) {
  case state.hello_received {
    False ->
      case msg {
        Hello(protocol: "pestcontrol", version: 1) ->
          Ok(ClientState(..state, hello_received: True))
        Hello(_, _) -> Error("Invalid Hello: wrong protocol or version")
        _ -> Error("Expected Hello message")
      }
    True ->
      case msg {
        SiteVisit(site:, populations:) -> {
          use validated <- result.try(validate_populations(populations))
          process.send(
            site_manager,
            HandleSiteVisit(site:, populations: validated),
          )
          Ok(state)
        }
        _ -> Error("Unexpected message type")
      }
  }
}

fn send_error_and_disconnect(
  conn: glisten.Connection(Nil),
  reason: String,
) -> glisten.Next(ClientState, glisten.Message(Nil)) {
  logging.log(logging.Debug, "Disconnecting client: " <> reason)
  let encoded = encode_message(MsgError(reason))
  let _ = glisten.send(conn, bytes_tree.from_bit_array(encoded))
  glisten.stop()
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let site_manager = start_site_manager()
  let assert Ok(_) =
    glisten.new(handle_connection(site_manager, _), handle_client_data)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
