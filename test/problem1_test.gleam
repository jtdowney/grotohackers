import gleam/json
import problem1.{Continue, Disconnect, FloatNumber, IntNumber, Request}

fn is_prime_request(number: Int) -> String {
  json.object([
    #("method", json.string("isPrime")),
    #("number", json.int(number)),
  ])
  |> json.to_string()
}

fn is_prime_response(prime: Bool) -> String {
  json.object([
    #("method", json.string("isPrime")),
    #("prime", json.bool(prime)),
  ])
  |> json.to_string()
}

fn wrong_method_request(method: String, number: Int) -> String {
  json.object([#("method", json.string(method)), #("number", json.int(number))])
  |> json.to_string()
}

pub fn request_decoder_int_test() {
  let request =
    json.object([
      #("method", json.string("isPrime")),
      #("number", json.int(123)),
    ])
    |> json.to_string()

  let request = problem1.request_decoder(request)
  assert request == Ok(Request(method: "isPrime", number: IntNumber(123)))
}

pub fn request_decoder_float_test() {
  let request =
    json.object([
      #("method", json.string("isPrime")),
      #("number", json.float(3.5)),
    ])
    |> json.to_string()

  let request = problem1.request_decoder(request)
  assert request == Ok(Request(method: "isPrime", number: FloatNumber(3.5)))
}

pub fn request_decoder_invalid_json_test() {
  let assert Error(_) = problem1.request_decoder("{invalid json}")
}

pub fn encode_prime_response_test() {
  let response = problem1.encode_prime_response(True)
  assert response
    == json.object([
      #("method", json.string("isPrime")),
      #("prime", json.bool(True)),
    ])
    |> json.to_string()

  let response = problem1.encode_prime_response(False)
  assert response
    == json.object([
      #("method", json.string("isPrime")),
      #("prime", json.bool(False)),
    ])
    |> json.to_string()
}

pub fn check_prime_with_prime_test() {
  assert problem1.check_prime(IntNumber(2))
  assert problem1.check_prime(IntNumber(17))
  assert problem1.check_prime(IntNumber(97))
}

pub fn check_prime_with_non_prime_test() {
  assert !problem1.check_prime(IntNumber(1))
  assert !problem1.check_prime(IntNumber(4))
  assert !problem1.check_prime(IntNumber(100))
}

pub fn check_prime_with_negative_test() {
  assert !problem1.check_prime(IntNumber(-5))
  assert !problem1.check_prime(IntNumber(-1))
}

pub fn check_prime_with_float_test() {
  assert !problem1.check_prime(FloatNumber(3.0))
  assert !problem1.check_prime(FloatNumber(3.5))
}

pub fn process_request_valid_prime_test() {
  let result = problem1.process_request(is_prime_request(17))
  assert result == Ok(True)
}

pub fn process_request_valid_non_prime_test() {
  let result = problem1.process_request(is_prime_request(4))
  assert result == Ok(False)
}

pub fn process_request_wrong_method_test() {
  let result = problem1.process_request(wrong_method_request("foo", 5))
  assert result == Error(Nil)
}

pub fn process_request_invalid_json_test() {
  let result = problem1.process_request("{invalid}")
  assert result == Error(Nil)
}

pub fn process_buffer_single_complete_line_test() {
  let assert Continue(responses:, buffer:) =
    problem1.process_buffer("", is_prime_request(17) <> "\n")
  assert buffer == ""
  assert responses == [is_prime_response(True) <> "\n"]
}

pub fn process_buffer_partial_line_test() {
  let assert Continue(responses:, buffer:) =
    problem1.process_buffer("", "{\"method\":\"isPrime\"")
  assert buffer == "{\"method\":\"isPrime\""
  assert responses == []
}

pub fn process_buffer_complete_partial_test() {
  // Testing partial buffer completion - keep raw strings for partial JSON fragments
  let assert Continue(responses:, buffer:) =
    problem1.process_buffer("{\"method\":\"isPrime\"", ",\"number\":17}\n")
  assert buffer == ""
  assert responses == [is_prime_response(True) <> "\n"]
}

pub fn process_buffer_multiple_lines_test() {
  let input = is_prime_request(17) <> "\n" <> is_prime_request(4) <> "\n"
  let assert Continue(responses:, buffer:) = problem1.process_buffer("", input)
  assert buffer == ""
  assert responses
    == [is_prime_response(True) <> "\n", is_prime_response(False) <> "\n"]
}

pub fn process_buffer_malformed_disconnects_test() {
  let assert Disconnect(response:) = problem1.process_buffer("", "{invalid}\n")
  assert response == "ERROR\n"
}

pub fn process_buffer_wrong_method_disconnects_test() {
  let assert Disconnect(response:) =
    problem1.process_buffer("", wrong_method_request("foo", 5) <> "\n")
  assert response == "ERROR\n"
}
