import gleam/bytes_tree
import gleam/erlang/process
import gleam/int
import gleam/option
import glisten.{Packet}
import logging

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let assert Ok(_) =
    glisten.new(
      fn(conn) {
        let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
          glisten.get_client_info(conn)
        logging.log(
          logging.Debug,
          "New connection from "
            <> glisten.ip_address_to_string(ip_address)
            <> " on "
            <> int.to_string(port),
        )

        #(Nil, option.None)
      },
      fn(state, msg, conn) {
        let assert Packet(msg) = msg
        let assert Ok(_) = glisten.send(conn, bytes_tree.from_bit_array(msg))

        glisten.continue(state)
      },
    )
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
