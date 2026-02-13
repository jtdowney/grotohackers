import gleam/erlang/process
import gleam/set
import problem6.{
  type ServerMessage, ErrorMsg, HeartbeatMsg, IAmCamera, IAmDispatcher,
  NeedMoreData, Plate, Ticket, TicketMsg, UnknownMessage, WantHeartbeat,
}

pub fn parse_string_basic_test() {
  let data = <<4, "test":utf8>>
  let assert Ok(#(str, rest)) = problem6.parse_string(data)
  assert str == "test"
  assert rest == <<>>
}

pub fn parse_string_with_remaining_test() {
  let data = <<3, "abc":utf8, 0xFF>>
  let assert Ok(#(str, rest)) = problem6.parse_string(data)
  assert str == "abc"
  assert rest == <<0xFF>>
}

pub fn parse_string_empty_string_test() {
  let data = <<0, 0xFF>>
  let assert Ok(#(str, rest)) = problem6.parse_string(data)
  assert str == ""
  assert rest == <<0xFF>>
}

pub fn parse_string_need_more_data_test() {
  assert problem6.parse_string(<<>>) == Error(NeedMoreData)
  assert problem6.parse_string(<<5, "ab":utf8>>) == Error(NeedMoreData)
}

pub fn parse_plate_message_test() {
  let data = <<0x20, 4, "UN1X":utf8, 0:size(32)-big>>
  let assert Ok(#(msg, rest)) = problem6.parse_message(data)
  assert msg == Plate(plate: "UN1X", timestamp: 0)
  assert rest == <<>>
}

pub fn parse_plate_with_timestamp_test() {
  let data = <<0x20, 4, "UN1X":utf8, 1000:size(32)-big>>
  let assert Ok(#(msg, _)) = problem6.parse_message(data)
  assert msg == Plate(plate: "UN1X", timestamp: 1000)
}

pub fn parse_want_heartbeat_test() {
  let data = <<0x40, 0:size(32)-big>>
  let assert Ok(#(msg, rest)) = problem6.parse_message(data)
  assert msg == WantHeartbeat(interval: 0)
  assert rest == <<>>
}

pub fn parse_want_heartbeat_nonzero_test() {
  let data = <<0x40, 10:size(32)-big>>
  let assert Ok(#(msg, _)) = problem6.parse_message(data)
  assert msg == WantHeartbeat(interval: 10)
}

pub fn parse_i_am_camera_test() {
  let data = <<0x80, 66:size(16)-big, 8:size(16)-big, 60:size(16)-big>>
  let assert Ok(#(msg, rest)) = problem6.parse_message(data)
  assert msg == IAmCamera(road: 66, mile: 8, limit: 60)
  assert rest == <<>>
}

pub fn parse_i_am_dispatcher_test() {
  let data = <<0x81, 3, 66:size(16)-big, 368:size(16)-big, 1:size(16)-big>>
  let assert Ok(#(msg, rest)) = problem6.parse_message(data)
  assert msg == IAmDispatcher(roads: [66, 368, 1])
  assert rest == <<>>
}

pub fn parse_i_am_dispatcher_zero_roads_test() {
  let data = <<0x81, 0>>
  let assert Ok(#(msg, rest)) = problem6.parse_message(data)
  assert msg == IAmDispatcher(roads: [])
  assert rest == <<>>
}

pub fn parse_message_need_more_data_test() {
  assert problem6.parse_message(<<>>) == Error(NeedMoreData)
  assert problem6.parse_message(<<0x20>>) == Error(NeedMoreData)
  assert problem6.parse_message(<<0x40, 0, 0>>) == Error(NeedMoreData)
  assert problem6.parse_message(<<0x80, 0, 1>>) == Error(NeedMoreData)
}

pub fn parse_message_unknown_type_test() {
  assert problem6.parse_message(<<0x99, 0, 0>>) == Error(UnknownMessage(0x99))
  assert problem6.parse_message(<<0x10, 0, 0>>) == Error(UnknownMessage(0x10))
}

pub fn process_buffer_single_message_test() {
  let data = <<0x40, 100:size(32)-big>>
  let assert Ok(#(messages, remainder)) = problem6.process_buffer(<<>>, data)
  assert messages == [WantHeartbeat(interval: 100)]
  assert remainder == <<>>
}

pub fn process_buffer_partial_data_test() {
  let data = <<0x40, 0, 0>>
  let assert Ok(#(messages, remainder)) = problem6.process_buffer(<<>>, data)
  assert messages == []
  assert remainder == data
}

pub fn process_buffer_multiple_messages_test() {
  let data = <<
    0x80, 66:size(16)-big, 8:size(16)-big, 60:size(16)-big, 0x40,
    25:size(32)-big,
  >>
  let assert Ok(#(messages, remainder)) = problem6.process_buffer(<<>>, data)
  assert messages
    == [IAmCamera(road: 66, mile: 8, limit: 60), WantHeartbeat(interval: 25)]
  assert remainder == <<>>
}

pub fn process_buffer_partial_continuation_test() {
  let first = <<0x40, 0, 0>>
  let assert Ok(#(msgs1, buffer)) = problem6.process_buffer(<<>>, first)
  assert msgs1 == []

  let rest = <<0, 42>>
  let assert Ok(#(msgs2, remainder)) = problem6.process_buffer(buffer, rest)
  assert msgs2 == [WantHeartbeat(interval: 42)]
  assert remainder == <<>>
}

pub fn process_buffer_unknown_type_error_test() {
  let data = <<0x99, 0, 0, 0, 0>>
  let assert Error(_) = problem6.process_buffer(<<>>, data)
}

pub fn encode_error_message_test() {
  let encoded = problem6.encode_message(ErrorMsg("bad"))
  assert encoded == <<0x10, 3, "bad":utf8>>
}

pub fn encode_error_message_empty_test() {
  let encoded = problem6.encode_message(ErrorMsg(""))
  assert encoded == <<0x10, 0>>
}

pub fn encode_heartbeat_test() {
  assert problem6.encode_message(HeartbeatMsg) == <<0x41>>
}

pub fn encode_ticket_test() {
  let ticket =
    Ticket(
      plate: "UN1X",
      road: 66,
      mile1: 100,
      timestamp1: 123_456,
      mile2: 110,
      timestamp2: 123_816,
      speed: 10_000,
    )
  let encoded = problem6.encode_message(TicketMsg(ticket))
  assert encoded
    == <<
      0x21, 4, "UN1X":utf8, 66:size(16)-big, 100:size(16)-big,
      123_456:size(32)-big, 110:size(16)-big, 123_816:size(32)-big,
      10_000:size(16)-big,
    >>
}

pub fn calculate_speed_basic_test() {
  let speed = problem6.calculate_speed(8, 0, 9, 45)
  assert speed == 8000
}

pub fn calculate_speed_ordering_invariance_test() {
  let speed1 = problem6.calculate_speed(8, 0, 9, 45)
  let speed2 = problem6.calculate_speed(9, 45, 8, 0)
  assert speed1 == speed2
}

pub fn calculate_speed_zero_time_test() {
  assert problem6.calculate_speed(100, 50, 200, 50) == 0
}

pub fn calculate_speed_spec_example_test() {
  let speed = problem6.calculate_speed(8, 0, 9, 45)
  assert speed == 8000
}

pub fn days_covered_same_day_test() {
  let days = problem6.days_covered(0, 100)
  assert days == set.from_list([0])
}

pub fn days_covered_multi_day_test() {
  let days = problem6.days_covered(0, 86_400 * 2 + 100)
  assert days == set.from_list([0, 1, 2])
}

pub fn days_covered_boundary_at_86400_test() {
  let days = problem6.days_covered(86_399, 86_400)
  assert days == set.from_list([0, 1])
}

pub fn days_covered_ordering_test() {
  let days1 = problem6.days_covered(0, 86_400)
  let days2 = problem6.days_covered(86_400, 0)
  assert days1 == days2
}

pub fn days_covered_same_timestamp_test() {
  let days = problem6.days_covered(86_400, 86_400)
  assert days == set.from_list([1])
}

pub fn actor_ticket_generation_test() {
  let server = problem6.start_server()
  let dispatcher_subject: process.Subject(ServerMessage) = process.new_subject()

  process.send(
    server,
    problem6.RegisterDispatcher(roads: [123], subject: dispatcher_subject),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "UN1X",
      road: 123,
      mile: 8,
      timestamp: 0,
      limit: 60,
    ),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "UN1X",
      road: 123,
      mile: 9,
      timestamp: 45,
      limit: 60,
    ),
  )

  let assert Ok(TicketMsg(ticket)) = process.receive(dispatcher_subject, 1000)
  assert ticket.plate == "UN1X"
  assert ticket.road == 123
  assert ticket.speed == 8000
}

pub fn actor_no_ticket_under_limit_test() {
  let server = problem6.start_server()
  let dispatcher_subject: process.Subject(ServerMessage) = process.new_subject()

  process.send(
    server,
    problem6.RegisterDispatcher(roads: [123], subject: dispatcher_subject),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "SLOW",
      road: 123,
      mile: 100,
      timestamp: 0,
      limit: 60,
    ),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "SLOW",
      road: 123,
      mile: 101,
      timestamp: 3600,
      limit: 60,
    ),
  )

  process.sleep(50)
  assert process.receive(dispatcher_subject, 100) == Error(Nil)
}

pub fn actor_day_deduplication_test() {
  let server = problem6.start_server()
  let dispatcher_subject: process.Subject(ServerMessage) = process.new_subject()

  process.send(
    server,
    problem6.RegisterDispatcher(roads: [1], subject: dispatcher_subject),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "DUPE",
      road: 1,
      mile: 0,
      timestamp: 0,
      limit: 10,
    ),
  )
  process.send(
    server,
    problem6.RecordObservation(
      plate: "DUPE",
      road: 1,
      mile: 100,
      timestamp: 100,
      limit: 10,
    ),
  )

  let assert Ok(TicketMsg(_)) = process.receive(dispatcher_subject, 1000)

  process.send(
    server,
    problem6.RecordObservation(
      plate: "DUPE",
      road: 1,
      mile: 200,
      timestamp: 200,
      limit: 10,
    ),
  )

  process.sleep(50)
  assert process.receive(dispatcher_subject, 100) == Error(Nil)
}

pub fn actor_pending_ticket_delivery_test() {
  let server = problem6.start_server()

  process.send(
    server,
    problem6.RecordObservation(
      plate: "WAIT",
      road: 42,
      mile: 8,
      timestamp: 0,
      limit: 60,
    ),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "WAIT",
      road: 42,
      mile: 9,
      timestamp: 45,
      limit: 60,
    ),
  )

  process.sleep(50)

  let dispatcher_subject: process.Subject(ServerMessage) = process.new_subject()
  process.send(
    server,
    problem6.RegisterDispatcher(roads: [42], subject: dispatcher_subject),
  )

  let assert Ok(TicketMsg(ticket)) = process.receive(dispatcher_subject, 1000)
  assert ticket.plate == "WAIT"
  assert ticket.road == 42
  assert ticket.speed == 8000
}

pub fn actor_dispatcher_disconnect_test() {
  let server = problem6.start_server()
  let dispatcher1: process.Subject(ServerMessage) = process.new_subject()

  process.send(
    server,
    problem6.RegisterDispatcher(roads: [7], subject: dispatcher1),
  )

  process.send(server, problem6.UnregisterDispatcher(dispatcher1))

  process.send(
    server,
    problem6.RecordObservation(
      plate: "DISC",
      road: 7,
      mile: 0,
      timestamp: 0,
      limit: 10,
    ),
  )
  process.send(
    server,
    problem6.RecordObservation(
      plate: "DISC",
      road: 7,
      mile: 100,
      timestamp: 100,
      limit: 10,
    ),
  )

  process.sleep(50)
  assert process.receive(dispatcher1, 100) == Error(Nil)

  let dispatcher2: process.Subject(ServerMessage) = process.new_subject()
  process.send(
    server,
    problem6.RegisterDispatcher(roads: [7], subject: dispatcher2),
  )

  let assert Ok(TicketMsg(ticket)) = process.receive(dispatcher2, 1000)
  assert ticket.plate == "DISC"
}

pub fn actor_multi_day_ticket_blocks_all_days_test() {
  let server = problem6.start_server()
  let dispatcher_subject: process.Subject(ServerMessage) = process.new_subject()

  process.send(
    server,
    problem6.RegisterDispatcher(roads: [5], subject: dispatcher_subject),
  )

  process.send(
    server,
    problem6.RecordObservation(
      plate: "MULTI",
      road: 5,
      mile: 0,
      timestamp: 0,
      limit: 10,
    ),
  )
  process.send(
    server,
    problem6.RecordObservation(
      plate: "MULTI",
      road: 5,
      mile: 1000,
      timestamp: 86_400 + 100,
      limit: 10,
    ),
  )

  let assert Ok(TicketMsg(ticket)) = process.receive(dispatcher_subject, 1000)
  assert ticket.plate == "MULTI"

  process.send(
    server,
    problem6.RecordObservation(
      plate: "MULTI",
      road: 5,
      mile: 2000,
      timestamp: 86_400 + 200,
      limit: 10,
    ),
  )

  process.sleep(50)
  assert process.receive(dispatcher_subject, 100) == Error(Nil)
}

pub fn spec_camera_plate_example_test() {
  let camera_msg = <<0x80, 0x00, 0x42, 0x00, 0x08, 0x00, 0x3C>>
  let assert Ok(#(msg, <<>>)) = problem6.parse_message(camera_msg)
  assert msg == IAmCamera(road: 66, mile: 8, limit: 60)

  let plate_msg = <<0x20, 0x04, 0x55, 0x4E, 0x31, 0x58, 0x00, 0x00, 0x03, 0xE8>>
  let assert Ok(#(msg2, <<>>)) = problem6.parse_message(plate_msg)
  assert msg2 == Plate(plate: "UN1X", timestamp: 1000)
}

pub fn spec_dispatcher_example_test() {
  let data = <<0x81, 0x03, 0x00, 0x42, 0x01, 0x70, 0x13, 0x88>>
  let assert Ok(#(msg, <<>>)) = problem6.parse_message(data)
  assert msg == IAmDispatcher(roads: [66, 368, 5000])
}

pub fn spec_ticket_encoding_test() {
  let ticket =
    Ticket(
      plate: "UN1X",
      road: 66,
      mile1: 8,
      timestamp1: 0,
      mile2: 9,
      timestamp2: 45,
      speed: 8000,
    )
  let encoded = problem6.encode_message(TicketMsg(ticket))
  assert encoded
    == <<
      0x21, 0x04, 0x55, 0x4E, 0x31, 0x58, 0x00, 0x42, 0x00, 0x08, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x2D, 0x1F, 0x40,
    >>
}

pub fn spec_error_encoding_test() {
  let encoded = problem6.encode_message(ErrorMsg("bad"))
  assert encoded == <<0x10, 0x03, 0x62, 0x61, 0x64>>
}

pub fn spec_heartbeat_encoding_test() {
  assert problem6.encode_message(HeartbeatMsg) == <<0x41>>
}

pub fn spec_want_heartbeat_parse_test() {
  let data = <<0x40, 0x00, 0x00, 0x00, 0x0A>>
  let assert Ok(#(msg, <<>>)) = problem6.parse_message(data)
  assert msg == WantHeartbeat(interval: 10)
}
