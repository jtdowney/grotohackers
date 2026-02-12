import gleam/erlang/process
import problem3

pub fn validate_name_alphanumeric_test() {
  assert problem3.is_valid_name("alice")
  assert problem3.is_valid_name("Bob123")
  assert problem3.is_valid_name("A")
  assert problem3.is_valid_name("0")
}

pub fn validate_name_empty_test() {
  assert !problem3.is_valid_name("")
}

pub fn validate_name_spaces_test() {
  assert !problem3.is_valid_name("alice bob")
  assert !problem3.is_valid_name(" ")
}

pub fn validate_name_special_chars_test() {
  assert !problem3.is_valid_name("alice!")
  assert !problem3.is_valid_name("bob@home")
  assert !problem3.is_valid_name("name-with-dash")
  assert !problem3.is_valid_name("under_score")
}

pub fn validate_name_long_alphanumeric_test() {
  assert problem3.is_valid_name("abcdefghijklmnopqrstuvwxyz0123456789")
}

pub fn process_buffer_empty_test() {
  let #(lines, remainder) = problem3.process_buffer("", "")
  assert lines == []
  assert remainder == ""
}

pub fn process_buffer_single_line_test() {
  let #(lines, remainder) = problem3.process_buffer("", "hello\n")
  assert lines == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_partial_test() {
  let #(lines, remainder) = problem3.process_buffer("", "hello")
  assert lines == []
  assert remainder == "hello"
}

pub fn process_buffer_multi_line_test() {
  let #(lines, remainder) = problem3.process_buffer("", "line1\nline2\n")
  assert lines == ["line1", "line2"]
  assert remainder == ""
}

pub fn process_buffer_crlf_stripping_test() {
  let #(lines, remainder) = problem3.process_buffer("", "hello\r\n")
  assert lines == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_partial_continuation_test() {
  let #(lines1, buffer) = problem3.process_buffer("", "hel")
  assert lines1 == []

  let #(lines2, remainder) = problem3.process_buffer(buffer, "lo\n")
  assert lines2 == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_mixed_complete_and_partial_test() {
  let #(lines, remainder) = problem3.process_buffer("", "line1\npartial")
  assert lines == ["line1"]
  assert remainder == "partial"
}

pub fn format_join_message_test() {
  assert problem3.format_join_message("alice")
    == "* alice has entered the room\n"
}

pub fn format_leave_message_test() {
  assert problem3.format_leave_message("bob") == "* bob has left the room\n"
}

pub fn format_chat_message_test() {
  assert problem3.format_chat_message("alice", "hello everyone")
    == "[alice] hello everyone\n"
}

pub fn format_room_members_empty_test() {
  assert problem3.format_room_members([]) == "* The room contains: \n"
}

pub fn format_room_members_single_test() {
  assert problem3.format_room_members(["alice"])
    == "* The room contains: alice\n"
}

pub fn format_room_members_multiple_test() {
  assert problem3.format_room_members(["alice", "bob", "charlie"])
    == "* The room contains: alice, bob, charlie\n"
}

pub fn room_join_empty_test() {
  let room = problem3.start_room()
  let client = process.new_subject()

  let result =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "alice", subject: client, reply:)
    })
  assert result == Ok([])
}

pub fn room_join_with_existing_member_test() {
  let room = problem3.start_room()
  let alice_subject = process.new_subject()
  let bob_subject = process.new_subject()

  let assert Ok([]) =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "alice", subject: alice_subject, reply:)
    })

  let result =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "bob", subject: bob_subject, reply:)
    })
  assert result == Ok(["alice"])

  let assert Ok(join_msg) = process.receive(alice_subject, 1000)
  assert join_msg == "* bob has entered the room\n"
}

pub fn room_duplicate_name_test() {
  let room = problem3.start_room()
  let alice1 = process.new_subject()
  let alice2 = process.new_subject()

  let assert Ok([]) =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "alice", subject: alice1, reply:)
    })

  let result =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "alice", subject: alice2, reply:)
    })
  assert result == Error(Nil)
}

pub fn room_chat_broadcast_test() {
  let room = problem3.start_room()
  let alice_subject = process.new_subject()
  let bob_subject = process.new_subject()

  let assert Ok([]) =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "alice", subject: alice_subject, reply:)
    })

  let assert Ok(["alice"]) =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "bob", subject: bob_subject, reply:)
    })

  // Consume alice's join notification for bob
  let assert Ok(_) = process.receive(alice_subject, 1000)

  process.send(room, problem3.Chat(sender: "alice", text: "hello"))

  let assert Ok(chat_msg) = process.receive(bob_subject, 1000)
  assert chat_msg == "[alice] hello\n"

  let assert Error(Nil) = process.receive(alice_subject, 100)
}

pub fn room_leave_broadcast_test() {
  let room = problem3.start_room()
  let alice_subject = process.new_subject()
  let bob_subject = process.new_subject()

  let assert Ok([]) =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "alice", subject: alice_subject, reply:)
    })

  let assert Ok(["alice"]) =
    process.call(room, 1000, fn(reply) {
      problem3.Join(name: "bob", subject: bob_subject, reply:)
    })

  // Consume alice's join notification for bob
  let assert Ok(_) = process.receive(alice_subject, 1000)

  process.send(room, problem3.Leave(name: "bob"))

  let assert Ok(leave_msg) = process.receive(alice_subject, 1000)
  assert leave_msg == "* bob has left the room\n"

  let assert Error(Nil) = process.receive(bob_subject, 100)
}
