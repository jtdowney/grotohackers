import gleam/bytes_tree
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option
import glisten.{Packet}

pub fn main() -> Nil {
  let assert Ok(_) =
    glisten.new(
      fn(conn) {
        let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
          glisten.get_client_info(conn)
        io.println(
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
