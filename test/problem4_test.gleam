import gleam/dict
import problem4.{Insert, Retrieve}

pub fn parse_request_insert_test() {
  assert problem4.parse_request(<<"foo=bar":utf8>>) == Ok(Insert("foo", "bar"))
}

pub fn parse_request_insert_with_multiple_equals_test() {
  assert problem4.parse_request(<<"foo=bar=baz":utf8>>)
    == Ok(Insert("foo", "bar=baz"))
}

pub fn parse_request_insert_empty_value_test() {
  assert problem4.parse_request(<<"foo=":utf8>>) == Ok(Insert("foo", ""))
}

pub fn parse_request_insert_empty_key_test() {
  assert problem4.parse_request(<<"=foo":utf8>>) == Ok(Insert("", "foo"))
}

pub fn parse_request_insert_equals_in_value_test() {
  assert problem4.parse_request(<<"foo===":utf8>>) == Ok(Insert("foo", "=="))
}

pub fn parse_request_retrieve_test() {
  assert problem4.parse_request(<<"foo":utf8>>) == Ok(Retrieve("foo"))
}

pub fn parse_request_invalid_utf8_test() {
  assert problem4.parse_request(<<0xFF, 0xFE>>) == Error(Nil)
}

pub fn handle_request_insert_test() {
  let store = dict.new()
  let #(new_store, response) =
    problem4.handle_request(store, Insert("foo", "bar"))
  assert dict.get(new_store, "foo") == Ok("bar")
  assert response == Error(Nil)
}

pub fn handle_request_retrieve_existing_test() {
  let store = dict.from_list([#("foo", "bar")])
  let #(_, response) = problem4.handle_request(store, Retrieve("foo"))
  assert response == Ok("foo=bar")
}

pub fn handle_request_retrieve_missing_test() {
  let store = dict.new()
  let #(_, response) = problem4.handle_request(store, Retrieve("foo"))
  assert response == Ok("foo=")
}

pub fn handle_request_version_immutable_test() {
  let store = dict.from_list([#("version", "grotohackers 1.0")])
  let #(new_store, response) =
    problem4.handle_request(store, Insert("version", "hacked"))
  assert dict.get(new_store, "version") == Ok("grotohackers 1.0")
  assert response == Error(Nil)
}

pub fn handle_request_overwrite_existing_test() {
  let store = dict.from_list([#("foo", "bar")])
  let #(new_store, response) =
    problem4.handle_request(store, Insert("foo", "baz"))
  assert dict.get(new_store, "foo") == Ok("baz")
  assert response == Error(Nil)
}
