import problem2.{Insert, Query}

pub fn parse_message_valid_insert_test() {
  let data = <<73:8, 12_345:size(32)-big, 101:size(32)-big>>
  assert problem2.parse_message(data)
    == Ok(Insert(timestamp: 12_345, price: 101))
}

pub fn parse_message_valid_query_test() {
  let data = <<81:8, 1000:size(32)-big, 2000:size(32)-big>>
  assert problem2.parse_message(data) == Ok(Query(mintime: 1000, maxtime: 2000))
}

pub fn parse_message_invalid_type_byte_test() {
  let data = <<65:8, 0:size(32)-big, 0:size(32)-big>>
  assert problem2.parse_message(data) == Error(Nil)
}

pub fn parse_message_too_short_test() {
  assert problem2.parse_message(<<73:8, 0:8>>) == Error(Nil)
}

pub fn parse_message_negative_values_test() {
  let data = <<73:8, -100:size(32)-big, -200:size(32)-big>>
  assert problem2.parse_message(data)
    == Ok(Insert(timestamp: -100, price: -200))
}

pub fn process_buffer_single_message_test() {
  let data = <<73:8, 1:size(32)-big, 50:size(32)-big>>
  let assert Ok(#(messages, remainder)) = problem2.process_buffer(<<>>, data)
  assert messages == [Insert(timestamp: 1, price: 50)]
  assert remainder == <<>>
}

pub fn process_buffer_partial_message_test() {
  let data = <<73:8, 0:8, 0:8>>
  let assert Ok(#(messages, remainder)) = problem2.process_buffer(<<>>, data)
  assert messages == []
  assert remainder == data
}

pub fn process_buffer_multiple_messages_test() {
  let data = <<
    73:8, 1:size(32)-big, 10:size(32)-big, 73:8, 2:size(32)-big, 20:size(32)-big,
  >>
  let assert Ok(#(messages, remainder)) = problem2.process_buffer(<<>>, data)
  assert messages
    == [Insert(timestamp: 1, price: 10), Insert(timestamp: 2, price: 20)]
  assert remainder == <<>>
}

pub fn process_buffer_partial_plus_complete_test() {
  let first = <<73:8, 0:8, 0:8>>
  let assert Ok(#(messages1, buffer)) = problem2.process_buffer(<<>>, first)
  assert messages1 == []

  let rest = <<0:8, 1:8, 0:8, 0:8, 0:8, 100:8>>
  let assert Ok(#(messages2, remainder)) = problem2.process_buffer(buffer, rest)
  assert messages2 == [Insert(timestamp: 1, price: 100)]
  assert remainder == <<>>
}

pub fn process_buffer_invalid_type_mid_stream_test() {
  let data = <<
    73:8, 1:size(32)-big, 10:size(32)-big, 65:8, 0:size(32)-big, 0:size(32)-big,
  >>
  assert problem2.process_buffer(<<>>, data) == Error(Nil)
}

pub fn compute_mean_basic_test() {
  let prices = [#(1, 10), #(2, 20), #(3, 30)]
  assert problem2.compute_mean(prices, 1, 3) == 20
}

pub fn compute_mean_empty_range_test() {
  let prices = [#(1, 10), #(2, 20)]
  assert problem2.compute_mean(prices, 5, 10) == 0
}

pub fn compute_mean_mintime_greater_than_maxtime_test() {
  let prices = [#(1, 10), #(2, 20)]
  assert problem2.compute_mean(prices, 10, 5) == 0
}

pub fn compute_mean_single_value_test() {
  let prices = [#(5, 42)]
  assert problem2.compute_mean(prices, 5, 5) == 42
}

pub fn compute_mean_truncation_toward_zero_test() {
  let prices = [#(1, 10), #(2, 11)]
  assert problem2.compute_mean(prices, 1, 2) == 10
}

pub fn compute_mean_negative_prices_test() {
  let prices = [#(1, -10), #(2, -20)]
  assert problem2.compute_mean(prices, 1, 2) == -15
}

pub fn compute_mean_boundary_timestamps_test() {
  let prices = [#(10, 100), #(20, 200), #(30, 300)]
  assert problem2.compute_mean(prices, 10, 20) == 150
}

pub fn process_messages_inserts_only_test() {
  let messages = [
    Insert(timestamp: 1, price: 10),
    Insert(timestamp: 2, price: 20),
  ]
  let #(prices, responses) = problem2.process_messages([], messages)
  assert prices == [#(2, 20), #(1, 10)]
  assert responses == []
}

pub fn process_messages_query_after_inserts_test() {
  let messages = [
    Insert(timestamp: 1, price: 10),
    Insert(timestamp: 2, price: 20),
    Query(mintime: 1, maxtime: 2),
  ]
  let #(_, responses) = problem2.process_messages([], messages)
  assert responses == [<<15:size(32)-big>>]
}

pub fn process_messages_mixed_sequence_test() {
  let messages = [
    Insert(timestamp: 1, price: 10),
    Query(mintime: 1, maxtime: 1),
    Insert(timestamp: 2, price: 20),
    Query(mintime: 1, maxtime: 2),
  ]
  let #(_, responses) = problem2.process_messages([], messages)
  assert responses == [<<10:size(32)-big>>, <<15:size(32)-big>>]
}

pub fn process_messages_empty_query_test() {
  let messages = [Query(mintime: 1, maxtime: 2)]
  let #(_, responses) = problem2.process_messages([], messages)
  assert responses == [<<0:size(32)-big>>]
}

pub fn spec_example_test() {
  let messages = [
    Insert(timestamp: 12_345, price: 101),
    Insert(timestamp: 12_346, price: 102),
    Insert(timestamp: 12_347, price: 100),
    Insert(timestamp: 40_960, price: 5),
    Query(mintime: 12_288, maxtime: 16_384),
  ]
  let #(_, responses) = problem2.process_messages([], messages)
  assert responses == [<<101:size(32)-big>>]
}
