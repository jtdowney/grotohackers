import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import gleam_community/maths
import glisten.{Packet}

pub type Number {
  IntNumber(Int)
  FloatNumber(Float)
}

pub type Request {
  Request(method: String, number: Number)
}

pub type State {
  State(buffer: String)
}

pub type LineResult {
  Continue(responses: List(String), buffer: String)
  Disconnect(response: String)
}

pub fn main() -> Nil {
  let assert Ok(_) =
    glisten.new(
      fn(conn) {
        let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
          glisten.get_client_info(conn)
        io.println(
          "New connection from "
          <> string.inspect(ip_address)
          <> " on "
          <> int.to_string(port),
        )

        #(State(buffer: ""), option.None)
      },
      fn(state, msg, conn) {
        let assert Packet(data) = msg
        handle_message(state, data, conn)
      },
    )
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}

fn handle_message(state: State, data: BitArray, conn) {
  let State(buffer:) = state

  case bit_array.to_string(data) {
    Error(_) -> send_and_disconnect(conn, "ERROR\n")
    Ok(text) ->
      case process_buffer(buffer, text) {
        Disconnect(response:) -> send_and_disconnect(conn, response)
        Continue(responses:, buffer: new_buffer) -> {
          list.each(responses, fn(response) {
            let _ = glisten.send(conn, bytes_tree.from_string(response))
          })
          glisten.continue(State(buffer: new_buffer))
        }
      }
  }
}

fn send_and_disconnect(conn, response) {
  let _ = glisten.send(conn, bytes_tree.from_string(response))
  glisten.stop()
}

fn number_decoder() -> decode.Decoder(Number) {
  decode.one_of(decode.int |> decode.map(IntNumber), or: [
    decode.float |> decode.map(FloatNumber),
  ])
}

pub fn request_decoder(json_string: String) -> Result(Request, json.DecodeError) {
  let decoder = {
    use method <- decode.field("method", decode.string)
    use number <- decode.field("number", number_decoder())
    decode.success(Request(method, number))
  }

  json.parse(from: json_string, using: decoder)
}

pub fn encode_prime_response(is_prime: Bool) -> String {
  json.object([
    #("method", json.string("isPrime")),
    #("prime", json.bool(is_prime)),
  ])
  |> json.to_string()
}

pub fn check_prime(number: Number) -> Bool {
  case number {
    FloatNumber(_) -> False
    IntNumber(n) -> {
      use <- bool.guard(when: n < 2, return: False)
      maths.is_prime(n)
    }
  }
}

pub fn process_request(line: String) -> Result(Bool, Nil) {
  use request <- result.try(request_decoder(line) |> result.replace_error(Nil))
  use <- bool.guard(when: request.method != "isPrime", return: Error(Nil))
  Ok(check_prime(request.number))
}

pub fn process_buffer(buffer: String, new_data: String) -> LineResult {
  let full_buffer = buffer <> new_data
  let parts = string.split(full_buffer, "\n")

  case list.reverse(parts) {
    [] -> Continue(responses: [], buffer: "")
    [remainder, ..complete_reversed] ->
      process_lines(list.reverse(complete_reversed), remainder)
  }
}

fn process_lines(lines: List(String), remainder: String) -> LineResult {
  let result =
    list.try_fold(lines, [], fn(acc, line) {
      use is_prime <- result.map(process_request(line))
      [encode_prime_response(is_prime) <> "\n", ..acc]
    })

  case result {
    Error(_) -> Disconnect(response: "ERROR\n")
    Ok(responses) -> Continue(responses: list.reverse(responses), buffer: remainder)
  }
}
