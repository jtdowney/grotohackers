import gleam/dict
import gleam/list
import problem11.{
  BadChecksum, Conserve, CreateNewPolicy, CreatePolicy, Cull,
  DeleteExistingPolicy, DeletePolicy, DialAuthority, Hello, InvalidMessage,
  MsgError, MsgOk, NeedMoreData, PolicyResult, PopulationObservation,
  PopulationTarget, SiteVisit, TargetPopulations, UnknownMessageType,
}

pub fn compute_checksum_all_zeros_test() {
  assert problem11.compute_checksum(<<0, 0, 0>>) == 0
}

pub fn compute_checksum_simple_test() {
  assert problem11.compute_checksum(<<1, 2, 3>>) == 250
}

pub fn compute_checksum_wrap_test() {
  assert problem11.compute_checksum(<<255, 1>>) == 0
}

pub fn verify_checksum_valid_test() {
  assert problem11.verify_checksum(<<1, 2, 3, 250>>) == True
}

pub fn verify_checksum_invalid_test() {
  assert problem11.verify_checksum(<<1, 2, 3, 4>>) == False
}

pub fn verify_checksum_all_zeros_test() {
  assert problem11.verify_checksum(<<0, 0, 0, 0>>) == True
}

pub fn parse_str_basic_test() {
  let data = <<4:size(32)-big, "test":utf8>>
  let assert Ok(#(str, rest)) = problem11.parse_str(data)
  assert str == "test"
  assert rest == <<>>
}

pub fn parse_str_with_remaining_test() {
  let data = <<3:size(32)-big, "abc":utf8, 0xFF>>
  let assert Ok(#(str, rest)) = problem11.parse_str(data)
  assert str == "abc"
  assert rest == <<0xFF>>
}

pub fn parse_str_empty_test() {
  let data = <<0:size(32)-big, 0xFF>>
  let assert Ok(#(str, rest)) = problem11.parse_str(data)
  assert str == ""
  assert rest == <<0xFF>>
}

pub fn parse_str_need_more_data_test() {
  assert problem11.parse_str(<<>>) == Error(NeedMoreData)
  assert problem11.parse_str(<<0, 0>>) == Error(NeedMoreData)
  assert problem11.parse_str(<<5:size(32)-big, "ab":utf8>>)
    == Error(NeedMoreData)
}

pub fn parse_hello_test() {
  let msg = problem11.encode_message(Hello(protocol: "pestcontrol", version: 1))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == Hello(protocol: "pestcontrol", version: 1)
  assert rest == <<>>
}

pub fn parse_error_test() {
  let msg = problem11.encode_message(MsgError(message: "bad"))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == MsgError(message: "bad")
  assert rest == <<>>
}

pub fn parse_ok_test() {
  let msg = problem11.encode_message(MsgOk)
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == MsgOk
  assert rest == <<>>
}

pub fn parse_dial_authority_test() {
  let msg = problem11.encode_message(DialAuthority(site: 12_345))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == DialAuthority(site: 12_345)
  assert rest == <<>>
}

pub fn parse_delete_policy_test() {
  let msg = problem11.encode_message(DeletePolicy(policy: 42))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == DeletePolicy(policy: 42)
  assert rest == <<>>
}

pub fn parse_create_policy_cull_test() {
  let msg = problem11.encode_message(CreatePolicy(species: "dog", action: Cull))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == CreatePolicy(species: "dog", action: Cull)
  assert rest == <<>>
}

pub fn parse_create_policy_conserve_test() {
  let msg =
    problem11.encode_message(CreatePolicy(species: "cat", action: Conserve))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == CreatePolicy(species: "cat", action: Conserve)
  assert rest == <<>>
}

pub fn parse_policy_result_test() {
  let msg = problem11.encode_message(PolicyResult(policy: 123))
  let assert Ok(#(parsed, rest)) = problem11.parse_message(msg)
  assert parsed == PolicyResult(policy: 123)
  assert rest == <<>>
}

pub fn parse_need_more_data_empty_test() {
  assert problem11.parse_message(<<>>) == Error(NeedMoreData)
}

pub fn parse_need_more_data_partial_header_test() {
  assert problem11.parse_message(<<0x50, 0, 0>>) == Error(NeedMoreData)
}

pub fn parse_need_more_data_incomplete_frame_test() {
  assert problem11.parse_message(<<0x50, 20:size(32)-big, 0, 0>>)
    == Error(NeedMoreData)
}

pub fn parse_bad_checksum_test() {
  let frame = <<0x52, 6:size(32)-big, 0xFF>>
  assert problem11.parse_message(frame) == Error(BadChecksum)
}

pub fn parse_unknown_message_type_test() {
  let without_checksum = <<0xFF, 6:size(32)-big>>
  let checksum = problem11.compute_checksum(without_checksum)
  let frame = <<without_checksum:bits, checksum:8>>
  assert problem11.parse_message(frame) == Error(UnknownMessageType(0xFF))
}

pub fn parse_length_too_short_test() {
  let frame = <<0x52, 0:size(32)-big, 0>>
  assert problem11.parse_message(frame)
    == Error(InvalidMessage("Message length too short"))
}

pub fn parse_length_5_too_short_test() {
  let frame = <<0x52, 5:size(32)-big, 0>>
  assert problem11.parse_message(frame)
    == Error(InvalidMessage("Message length too short"))
}

pub fn parse_length_too_large_test() {
  let frame = <<0x52, 0xFF, 0xFF, 0xFF, 0xFF, 0>>
  assert problem11.parse_message(frame)
    == Error(InvalidMessage("Message length too large"))
}

pub fn spec_hello_example_test() {
  let expected = <<
    0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74,
    0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
  >>
  let encoded =
    problem11.encode_message(Hello(protocol: "pestcontrol", version: 1))
  assert encoded == expected
}

pub fn spec_ok_example_test() {
  let data = <<0x52, 0x00, 0x00, 0x00, 0x06, 0xa8>>
  let assert Ok(#(msg, <<>>)) = problem11.parse_message(data)
  assert msg == MsgOk
}

pub fn spec_ok_encode_matches_test() {
  let encoded = problem11.encode_message(MsgOk)
  assert encoded == <<0x52, 0x00, 0x00, 0x00, 0x06, 0xa8>>
}

pub fn spec_target_populations_example_test() {
  let data = <<
    0x54, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02,
    0x00, 0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
    0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x0a, 0x80,
  >>
  let assert Ok(#(msg, <<>>)) = problem11.parse_message(data)
  assert msg
    == TargetPopulations(site: 12_345, populations: [
      PopulationTarget(species: "dog", min: 1, max: 3),
      PopulationTarget(species: "rat", min: 0, max: 10),
    ])
}

pub fn spec_site_visit_example_test() {
  let data = <<
    0x58, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02,
    0x00, 0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
    0x00, 0x03, 0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x05, 0x8c,
  >>
  let assert Ok(#(msg, <<>>)) = problem11.parse_message(data)
  assert msg
    == SiteVisit(site: 12_345, populations: [
      PopulationObservation(species: "dog", count: 1),
      PopulationObservation(species: "rat", count: 5),
    ])
}

pub fn buffer_single_message_test() {
  let msg = problem11.encode_message(MsgOk)
  let assert Ok(#(messages, remainder)) = problem11.process_buffer(<<>>, msg)
  assert messages == [MsgOk]
  assert remainder == <<>>
}

pub fn buffer_multiple_messages_test() {
  let msg1 =
    problem11.encode_message(Hello(protocol: "pestcontrol", version: 1))
  let msg2 = problem11.encode_message(MsgOk)
  let combined = <<msg1:bits, msg2:bits>>
  let assert Ok(#(messages, remainder)) =
    problem11.process_buffer(<<>>, combined)
  assert messages == [Hello(protocol: "pestcontrol", version: 1), MsgOk]
  assert remainder == <<>>
}

pub fn buffer_partial_message_test() {
  let msg = problem11.encode_message(MsgOk)
  let assert <<partial:bytes-size(3), rest:bytes>> = msg
  let assert Ok(#(messages, buffer)) = problem11.process_buffer(<<>>, partial)
  assert messages == []

  let assert Ok(#(messages2, remainder)) =
    problem11.process_buffer(buffer, rest)
  assert messages2 == [MsgOk]
  assert remainder == <<>>
}

pub fn buffer_split_across_packets_test() {
  let msg1 =
    problem11.encode_message(Hello(protocol: "pestcontrol", version: 1))
  let msg2 = problem11.encode_message(MsgOk)
  let combined = <<msg1:bits, msg2:bits>>

  let split_at = 25 + 3
  let assert <<first_part:bytes-size(split_at), second_part:bytes>> = combined
  let assert Ok(#(messages1, buffer)) =
    problem11.process_buffer(<<>>, first_part)
  assert messages1 == [Hello(protocol: "pestcontrol", version: 1)]

  let assert Ok(#(messages2, remainder)) =
    problem11.process_buffer(buffer, second_part)
  assert messages2 == [MsgOk]
  assert remainder == <<>>
}

pub fn buffer_bad_checksum_error_test() {
  let frame = <<0x52, 0x00, 0x00, 0x00, 0x06, 0xFF>>
  let assert Error(_) = problem11.process_buffer(<<>>, frame)
}

pub fn validate_populations_unique_test() {
  let pops = [
    PopulationObservation(species: "dog", count: 5),
    PopulationObservation(species: "cat", count: 3),
  ]
  let assert Ok(result) = problem11.validate_populations(pops)
  assert dict.get(result, "dog") == Ok(5)
  assert dict.get(result, "cat") == Ok(3)
}

pub fn validate_populations_non_conflicting_dupes_test() {
  let pops = [
    PopulationObservation(species: "dog", count: 5),
    PopulationObservation(species: "dog", count: 5),
  ]
  let assert Ok(result) = problem11.validate_populations(pops)
  assert dict.get(result, "dog") == Ok(5)
}

pub fn validate_populations_conflicting_dupes_test() {
  let pops = [
    PopulationObservation(species: "dog", count: 5),
    PopulationObservation(species: "dog", count: 10),
  ]
  let assert Error(_) = problem11.validate_populations(pops)
}

pub fn validate_populations_empty_test() {
  let assert Ok(result) = problem11.validate_populations([])
  assert dict.size(result) == 0
}

pub fn policy_changes_below_min_conserve_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 2)])
  let policies = dict.new()

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == [CreateNewPolicy(species: "dog", action: Conserve)]
}

pub fn policy_changes_above_max_cull_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 25)])
  let policies = dict.new()

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == [CreateNewPolicy(species: "dog", action: Cull)]
}

pub fn policy_changes_in_range_nothing_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 10)])
  let policies = dict.new()

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == []
}

pub fn policy_changes_in_range_delete_existing_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 10)])
  let policies = dict.from_list([#("dog", #(42, Conserve))])

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == [DeleteExistingPolicy(species: "dog", policy_id: 42)]
}

pub fn policy_changes_same_action_no_change_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 2)])
  let policies = dict.from_list([#("dog", #(42, Conserve))])

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == []
}

pub fn policy_changes_different_action_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 25)])
  let policies = dict.from_list([#("dog", #(42, Conserve))])

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert list.contains(changes, CreateNewPolicy(species: "dog", action: Cull))
  assert list.contains(
    changes,
    DeleteExistingPolicy(species: "dog", policy_id: 42),
  )
}

pub fn policy_changes_missing_observation_is_zero_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.new()
  let policies = dict.new()

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == [CreateNewPolicy(species: "dog", action: Conserve)]
}

pub fn policy_changes_species_not_in_targets_ignored_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations = dict.from_list([#("dog", 10), #("cat", 100)])
  let policies = dict.new()

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert changes == []
}

pub fn policy_changes_at_boundary_no_action_test() {
  let targets = dict.from_list([#("dog", #(5, 20))])
  let observations_at_min = dict.from_list([#("dog", 5)])
  let observations_at_max = dict.from_list([#("dog", 20)])
  let policies = dict.new()

  let changes1 =
    problem11.compute_policy_changes(targets, observations_at_min, policies)
  assert changes1 == []

  let changes2 =
    problem11.compute_policy_changes(targets, observations_at_max, policies)
  assert changes2 == []
}

pub fn policy_changes_multiple_species_test() {
  let targets =
    dict.from_list([
      #("dog", #(5, 20)),
      #("cat", #(0, 10)),
      #("rat", #(1, 5)),
    ])
  let observations = dict.from_list([#("dog", 2), #("cat", 7), #("rat", 10)])
  let policies = dict.new()

  let changes =
    problem11.compute_policy_changes(targets, observations, policies)
  assert list.contains(
    changes,
    CreateNewPolicy(species: "dog", action: Conserve),
  )
  assert list.contains(changes, CreateNewPolicy(species: "rat", action: Cull))

  let cat_changes =
    list.filter(changes, fn(c) {
      case c {
        CreateNewPolicy(species: "cat", ..) -> True
        DeleteExistingPolicy(species: "cat", ..) -> True
        _ -> False
      }
    })
  assert cat_changes == []
}
