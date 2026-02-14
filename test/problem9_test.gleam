import gleam/dynamic
import gleam/erlang/process
import gleam/json
import problem9

pub fn parse_put_request_test() {
  let input =
    json.object([
      #("request", json.string("put")),
      #("queue", json.string("q1")),
      #("job", json.object([#("title", json.string("test"))])),
      #("pri", json.int(10)),
    ])
    |> json.to_string()
  let assert Ok(problem9.Put(queue: "q1", priority: 10, ..)) =
    problem9.parse_request(input)
}

pub fn parse_put_with_numeric_job_test() {
  let input =
    json.object([
      #("request", json.string("put")),
      #("queue", json.string("q1")),
      #("job", json.int(123)),
      #("pri", json.int(5)),
    ])
    |> json.to_string()
  let assert Ok(problem9.Put(queue: "q1", priority: 5, ..)) =
    problem9.parse_request(input)
}

pub fn parse_get_request_test() {
  let input =
    json.object([
      #("request", json.string("get")),
      #("queues", json.array(["q1", "q2"], json.string)),
    ])
    |> json.to_string()
  let assert Ok(problem9.Get(queues: ["q1", "q2"], wait: False)) =
    problem9.parse_request(input)
}

pub fn parse_get_with_wait_test() {
  let input =
    json.object([
      #("request", json.string("get")),
      #("queues", json.array(["q1"], json.string)),
      #("wait", json.bool(True)),
    ])
    |> json.to_string()
  let assert Ok(problem9.Get(queues: ["q1"], wait: True)) =
    problem9.parse_request(input)
}

pub fn parse_get_defaults_wait_false_test() {
  let input =
    json.object([
      #("request", json.string("get")),
      #("queues", json.array(["q1"], json.string)),
    ])
    |> json.to_string()
  let assert Ok(problem9.Get(queues: ["q1"], wait: False)) =
    problem9.parse_request(input)
}

pub fn parse_delete_request_test() {
  let input =
    json.object([#("request", json.string("delete")), #("id", json.int(42))])
    |> json.to_string()
  let assert Ok(problem9.Delete(id: 42)) = problem9.parse_request(input)
}

pub fn parse_abort_request_test() {
  let input =
    json.object([#("request", json.string("abort")), #("id", json.int(7))])
    |> json.to_string()
  let assert Ok(problem9.Abort(id: 7)) = problem9.parse_request(input)
}

pub fn parse_unknown_request_test() {
  let input =
    json.object([#("request", json.string("unknown"))]) |> json.to_string()
  assert problem9.parse_request(input) == Error(Nil)
}

pub fn parse_invalid_json_test() {
  assert problem9.parse_request("not json") == Error(Nil)
}

pub fn parse_missing_fields_test() {
  let input =
    json.object([
      #("request", json.string("put")),
      #("queue", json.string("q1")),
    ])
    |> json.to_string()
  assert problem9.parse_request(input) == Error(Nil)
}

pub fn encode_put_ok_test() {
  let result = problem9.encode_response(problem9.PutOk(id: 1))
  let expected =
    json.object([#("status", json.string("ok")), #("id", json.int(1))])
    |> json.to_string()
  assert result == expected
}

pub fn encode_no_job_test() {
  let result = problem9.encode_response(problem9.NoJob)
  let expected =
    json.object([#("status", json.string("no-job"))]) |> json.to_string()
  assert result == expected
}

pub fn encode_ok_response_test() {
  let result = problem9.encode_response(problem9.OkResponse)
  let expected =
    json.object([#("status", json.string("ok"))]) |> json.to_string()
  assert result == expected
}

pub fn encode_error_response_test() {
  let result =
    problem9.encode_response(problem9.ErrorResponse("something went wrong"))
  let expected =
    json.object([
      #("status", json.string("error")),
      #("error", json.string("something went wrong")),
    ])
    |> json.to_string()
  assert result == expected
}

pub fn encode_get_ok_test() {
  let job_json = json.object([#("title", json.string("test"))])
  let input =
    json.object([
      #("request", json.string("put")),
      #("queue", json.string("q1")),
      #("job", job_json),
      #("pri", json.int(10)),
    ])
    |> json.to_string()
  let assert Ok(problem9.Put(job: job_data, ..)) = problem9.parse_request(input)
  let result =
    problem9.encode_response(problem9.GetOk(
      id: 1,
      job: job_data,
      priority: 10,
      queue: "q1",
    ))
  let expected =
    json.object([
      #("status", json.string("ok")),
      #("id", json.int(1)),
      #("job", job_json),
      #("pri", json.int(10)),
      #("queue", json.string("q1")),
    ])
    |> json.to_string()
  assert result == expected
}

pub fn process_buffer_empty_test() {
  let #(lines, remainder) = problem9.process_buffer("", "")
  assert lines == []
  assert remainder == ""
}

pub fn process_buffer_single_line_test() {
  let #(lines, remainder) = problem9.process_buffer("", "hello\n")
  assert lines == ["hello"]
  assert remainder == ""
}

pub fn process_buffer_partial_test() {
  let #(lines, remainder) = problem9.process_buffer("", "hello")
  assert lines == []
  assert remainder == "hello"
}

pub fn process_buffer_multi_line_test() {
  let #(lines, remainder) = problem9.process_buffer("", "line1\nline2\n")
  assert lines == ["line1", "line2"]
  assert remainder == ""
}

pub fn process_buffer_continuation_test() {
  let #(lines1, buffer) = problem9.process_buffer("", "hel")
  assert lines1 == []

  let #(lines2, remainder) = problem9.process_buffer(buffer, "lo\n")
  assert lines2 == ["hello"]
  assert remainder == ""
}

pub fn put_and_get_basic_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let put_response =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.int(42),
        priority: 10,
        reply:,
      )
    })
  let assert problem9.PutOk(id: job_id) = put_response

  let get_response =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })
  let assert problem9.GetOk(id: got_id, priority: 10, queue: "q1", ..) =
    get_response
  assert got_id == job_id
}

pub fn priority_ordering_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: low_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("low"),
        priority: 1,
        reply:,
      )
    })

  let assert problem9.PutOk(id: high_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("high"),
        priority: 100,
        reply:,
      )
    })

  let assert problem9.GetOk(id: got_id, ..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })
  assert got_id == high_id

  let assert problem9.GetOk(id: got_id2, ..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })
  assert got_id2 == low_id
}

pub fn multi_queue_get_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q2",
        job_data: dynamic.string("job"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.GetOk(id: got_id, queue: "q2", ..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1", "q2"], wait: False, reply:)
    })
  assert got_id == job_id
}

pub fn get_no_job_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.NoJob =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })
}

pub fn delete_removes_job_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.OkResponse =
    process.call(queue, 1000, fn(reply) {
      problem9.DeleteJob(id: job_id, reply:)
    })

  let assert problem9.NoJob =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })
}

pub fn delete_nonexistent_job_test() {
  let queue = problem9.start_queue()

  let assert problem9.NoJob =
    process.call(queue, 1000, fn(reply) { problem9.DeleteJob(id: 9999, reply:) })
}

pub fn abort_requeues_job_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.GetOk(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })

  let assert problem9.OkResponse =
    process.call(queue, 1000, fn(reply) {
      problem9.AbortJob(client_id:, id: job_id, reply:)
    })

  let assert problem9.GetOk(id: got_id, ..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })
  assert got_id == job_id
}

pub fn abort_wrong_client_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client1 = problem9.next_id(counter)
  let client2 = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.GetOk(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id: client1, queues: ["q1"], wait: False, reply:)
    })

  let assert problem9.ErrorResponse(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.AbortJob(client_id: client2, id: job_id, reply:)
    })
}

pub fn abort_deleted_job_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.OkResponse =
    process.call(queue, 1000, fn(reply) {
      problem9.DeleteJob(id: job_id, reply:)
    })

  let assert problem9.NoJob =
    process.call(queue, 1000, fn(reply) {
      problem9.AbortJob(client_id:, id: job_id, reply:)
    })
}

pub fn wait_then_put_satisfies_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let wait_reply = process.new_subject()
  process.send(
    queue,
    problem9.GetJob(client_id:, queues: ["q1"], wait: True, reply: wait_reply),
  )

  let assert Error(Nil) = process.receive(wait_reply, 50)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("waited"),
        priority: 1,
        reply:,
      )
    })

  let assert Ok(problem9.GetOk(id: got_id, queue: "q1", ..)) =
    process.receive(wait_reply, 1000)
  assert got_id == job_id
}

pub fn disconnect_aborts_working_jobs_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)
  let client2 = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.GetOk(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })

  process.send(queue, problem9.ClientDisconnect(client_id:))
  process.sleep(50)

  let assert problem9.GetOk(id: got_id, ..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id: client2, queues: ["q1"], wait: False, reply:)
    })
  assert got_id == job_id
}

pub fn disconnect_removes_waiters_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let wait_reply = process.new_subject()
  process.send(
    queue,
    problem9.GetJob(client_id:, queues: ["q1"], wait: True, reply: wait_reply),
  )

  process.send(queue, problem9.ClientDisconnect(client_id:))
  process.sleep(50)

  let assert problem9.PutOk(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert Error(Nil) = process.receive(wait_reply, 100)
}

pub fn delete_working_job_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.GetOk(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.GetJob(client_id:, queues: ["q1"], wait: False, reply:)
    })

  let assert problem9.OkResponse =
    process.call(queue, 1000, fn(reply) {
      problem9.DeleteJob(id: job_id, reply:)
    })

  let assert problem9.NoJob =
    process.call(queue, 1000, fn(reply) {
      problem9.AbortJob(client_id:, id: job_id, reply:)
    })
}

pub fn abort_not_working_job_test() {
  let queue = problem9.start_queue()
  let counter = problem9.new_counter()

  let client_id = problem9.next_id(counter)

  let assert problem9.PutOk(id: job_id) =
    process.call(queue, 1000, fn(reply) {
      problem9.PutJob(
        queue: "q1",
        job_data: dynamic.string("data"),
        priority: 5,
        reply:,
      )
    })

  let assert problem9.ErrorResponse(..) =
    process.call(queue, 1000, fn(reply) {
      problem9.AbortJob(client_id:, id: job_id, reply:)
    })
}
