import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/string
import gleamy/priority_queue.{type Queue as PQueue}
import glisten
import logging

pub type Counter

@external(erlang, "atomics", "new")
fn atomics_new(size: Int, opts: List(a)) -> Counter

@external(erlang, "atomics", "add_get")
fn atomics_add_get(counter: Counter, index: Int, incr: Int) -> Int

pub fn new_counter() -> Counter {
  atomics_new(1, [])
}

pub fn next_id(counter: Counter) -> Int {
  atomics_add_get(counter, 1, 1)
}

pub type Job {
  Job(queue: String, data: Dynamic, priority: Int)
}

pub type Request {
  Put(queue: String, job: Dynamic, priority: Int)
  Get(queues: List(String), wait: Bool)
  Delete(id: Int)
  Abort(id: Int)
}

pub type Response {
  PutOk(id: Int)
  GetOk(id: Int, job: Dynamic, priority: Int, queue: String)
  NoJob
  OkResponse
  ErrorResponse(msg: String)
}

@external(erlang, "json", "encode")
fn encode_dynamic(value: Dynamic) -> json.Json

pub fn parse_request(input: String) -> Result(Request, Nil) {
  let request_type_decoder = {
    use request <- decode.field("request", decode.string)
    decode.success(request)
  }

  use request_type <- result.try(
    json.parse(input, request_type_decoder) |> result.replace_error(Nil),
  )

  case request_type {
    "put" -> parse_put(input)
    "get" -> parse_get(input)
    "delete" -> parse_delete(input)
    "abort" -> parse_abort(input)
    _ -> Error(Nil)
  }
}

fn parse_put(input: String) -> Result(Request, Nil) {
  let decoder = {
    use queue <- decode.field("queue", decode.string)
    use job <- decode.field("job", decode.dynamic)
    use pri <- decode.field("pri", decode.int)
    decode.success(Put(queue:, job:, priority: pri))
  }
  json.parse(input, decoder) |> result.replace_error(Nil)
}

fn parse_get(input: String) -> Result(Request, Nil) {
  let decoder = {
    use queues <- decode.field("queues", decode.list(decode.string))
    use wait <- decode.optional_field("wait", False, decode.bool)
    decode.success(Get(queues:, wait:))
  }
  json.parse(input, decoder) |> result.replace_error(Nil)
}

fn parse_delete(input: String) -> Result(Request, Nil) {
  let decoder = {
    use id <- decode.field("id", decode.int)
    decode.success(Delete(id:))
  }
  json.parse(input, decoder) |> result.replace_error(Nil)
}

fn parse_abort(input: String) -> Result(Request, Nil) {
  let decoder = {
    use id <- decode.field("id", decode.int)
    decode.success(Abort(id:))
  }
  json.parse(input, decoder) |> result.replace_error(Nil)
}

pub fn encode_response(response: Response) -> String {
  case response {
    PutOk(id:) ->
      json.object([#("status", json.string("ok")), #("id", json.int(id))])
    GetOk(id:, job:, priority:, queue:) ->
      json.object([
        #("status", json.string("ok")),
        #("id", json.int(id)),
        #("job", encode_dynamic(job)),
        #("pri", json.int(priority)),
        #("queue", json.string(queue)),
      ])
    NoJob -> json.object([#("status", json.string("no-job"))])
    OkResponse -> json.object([#("status", json.string("ok"))])
    ErrorResponse(msg:) ->
      json.object([
        #("status", json.string("error")),
        #("error", json.string(msg)),
      ])
  }
  |> json.to_string()
}

pub fn process_buffer(
  buffer: String,
  new_data: String,
) -> #(List(String), String) {
  let full_buffer = buffer <> new_data
  let parts = string.split(full_buffer, "\n")

  case list.reverse(parts) {
    [] -> #([], "")
    [remainder, ..complete_reversed] -> {
      let lines =
        list.reverse(complete_reversed)
        |> list.map(string.replace(_, "\r", ""))
      #(lines, remainder)
    }
  }
}

pub type Waiter {
  Waiter(client_id: Int, queues: List(String), reply: Subject(Response))
}

pub type QueueState {
  QueueState(
    next_id: Int,
    next_client_id: Int,
    jobs: Dict(Int, Job),
    queues: Dict(String, PQueue(#(Int, Int))),
    working: Dict(Int, Int),
    client_jobs: Dict(Int, List(Int)),
    waiters: List(Waiter),
  )
}

fn pq_compare(a: #(Int, Int), b: #(Int, Int)) -> order.Order {
  let #(pri_a, _) = a
  let #(pri_b, _) = b
  int.compare(pri_a, pri_b) |> order.negate
}

fn new_pqueue() -> PQueue(#(Int, Int)) {
  priority_queue.new(pq_compare)
}

pub type QueueMessage {
  PutJob(
    queue: String,
    job_data: Dynamic,
    priority: Int,
    reply: Subject(Response),
  )
  GetJob(
    client_id: Int,
    queues: List(String),
    wait: Bool,
    reply: Subject(Response),
  )
  DeleteJob(id: Int, reply: Subject(Response))
  AbortJob(client_id: Int, id: Int, reply: Subject(Response))
  ClientDisconnect(client_id: Int)
  EchoReply(response: Response, reply: Subject(Response))
}

fn new_queue_state() -> QueueState {
  QueueState(
    next_id: 1,
    next_client_id: 1,
    jobs: dict.new(),
    queues: dict.new(),
    working: dict.new(),
    client_jobs: dict.new(),
    waiters: [],
  )
}

pub fn start_queue() -> Subject(QueueMessage) {
  let assert Ok(started) =
    actor.new(new_queue_state())
    |> actor.on_message(handle_queue_message)
    |> actor.start

  started.data
}

fn handle_queue_message(
  state: QueueState,
  msg: QueueMessage,
) -> actor.Next(QueueState, QueueMessage) {
  case msg {
    PutJob(queue:, job_data:, priority:, reply:) ->
      handle_put(state, queue, job_data, priority, reply)
    GetJob(client_id:, queues:, wait:, reply:) ->
      handle_get(state, client_id, queues, wait, reply)
    DeleteJob(id:, reply:) -> handle_delete(state, id, reply)
    AbortJob(client_id:, id:, reply:) ->
      handle_abort(state, client_id, id, reply)
    ClientDisconnect(client_id:) -> handle_disconnect(state, client_id)
    EchoReply(response:, reply:) -> {
      process.send(reply, response)
      actor.continue(state)
    }
  }
}

fn handle_put(
  state: QueueState,
  queue: String,
  job_data: Dynamic,
  priority: Int,
  reply: Subject(Response),
) -> actor.Next(QueueState, QueueMessage) {
  let id = state.next_id
  let job = Job(queue:, data: job_data, priority:)
  let jobs = dict.insert(state.jobs, id, job)
  let state = QueueState(..state, next_id: id + 1, jobs:)

  process.send(reply, PutOk(id:))

  let #(state, matched) = try_match_waiters(state, queue, id)
  use <- bool.lazy_guard(when: matched, return: fn() { actor.continue(state) })

  let state = enqueue_job(state, queue, id, priority)
  actor.continue(state)
}

fn enqueue_job(
  state: QueueState,
  queue: String,
  job_id: Int,
  priority: Int,
) -> QueueState {
  let pq = dict.get(state.queues, queue) |> result.unwrap(new_pqueue())
  let queues =
    dict.insert(
      state.queues,
      queue,
      priority_queue.push(pq, #(priority, job_id)),
    )
  QueueState(..state, queues:)
}

fn handle_get(
  state: QueueState,
  client_id: Int,
  queues: List(String),
  wait: Bool,
  reply: Subject(Response),
) -> actor.Next(QueueState, QueueMessage) {
  let #(state, found) = find_highest_priority_job(state, queues)
  case found {
    Ok(#(job_id, job)) -> {
      let state = assign_job(state, job_id, job, client_id)
      process.send(
        reply,
        GetOk(
          id: job_id,
          job: job.data,
          priority: job.priority,
          queue: job.queue,
        ),
      )
      actor.continue(state)
    }
    Error(_) -> {
      case wait {
        False -> {
          process.send(reply, NoJob)
          actor.continue(state)
        }
        True -> {
          let waiter = Waiter(client_id:, queues:, reply:)
          let waiters = list.append(state.waiters, [waiter])
          actor.continue(QueueState(..state, waiters:))
        }
      }
    }
  }
}

fn find_highest_priority_job(
  state: QueueState,
  queues: List(String),
) -> #(QueueState, Result(#(Int, Job), Nil)) {
  let #(state, candidates) =
    list.fold(queues, #(state, []), fn(acc, queue_name) {
      let #(state, candidates) = acc
      let #(state, result) = peek_valid_job(state, queue_name)
      case result {
        Ok(candidate) -> #(state, [candidate, ..candidates])
        Error(_) -> #(state, candidates)
      }
    })

  let result =
    candidates
    |> list.reduce(fn(best, entry) {
      let #(pri_best, _, _) = best
      let #(pri_entry, _, _) = entry
      case pri_entry > pri_best {
        True -> entry
        False -> best
      }
    })
    |> result.map(fn(entry) {
      let #(_, job_id, job) = entry
      #(job_id, job)
    })

  #(state, result)
}

fn peek_valid_job(
  state: QueueState,
  queue_name: String,
) -> #(QueueState, Result(#(Int, Int, Job), Nil)) {
  case dict.get(state.queues, queue_name) {
    Error(_) -> #(state, Error(Nil))
    Ok(pq) -> {
      let #(cleaned_pq, found) = clean_pq_and_peek(state, pq)
      let queues = dict.insert(state.queues, queue_name, cleaned_pq)
      #(QueueState(..state, queues:), found)
    }
  }
}

fn clean_pq_and_peek(
  state: QueueState,
  pq: PQueue(#(Int, Int)),
) -> #(PQueue(#(Int, Int)), Result(#(Int, Int, Job), Nil)) {
  case priority_queue.peek(pq) {
    Error(_) -> #(pq, Error(Nil))
    Ok(#(pri, job_id)) -> {
      case dict.get(state.jobs, job_id) {
        Ok(job) -> #(pq, Ok(#(pri, job_id, job)))
        Error(_) -> {
          let assert Ok(#(_, rest)) = priority_queue.pop(pq)
          clean_pq_and_peek(state, rest)
        }
      }
    }
  }
}

fn assign_job(
  state: QueueState,
  job_id: Int,
  job: Job,
  client_id: Int,
) -> QueueState {
  let pq = dict.get(state.queues, job.queue) |> result.unwrap(new_pqueue())
  // Pop the top entry only if it matches the assigned job. When the job was
  // matched via try_match_waiters it was never enqueued, so a mismatch (or
  // empty queue) is expected and silently kept as-is.
  let queues = case priority_queue.pop(pq) {
    Ok(#(#(_, id), rest)) if id == job_id ->
      dict.insert(state.queues, job.queue, rest)
    _ -> state.queues
  }
  let working = dict.insert(state.working, job_id, client_id)
  let client_existing =
    dict.get(state.client_jobs, client_id) |> result.unwrap([])
  let client_jobs =
    dict.insert(state.client_jobs, client_id, [job_id, ..client_existing])
  QueueState(..state, queues:, working:, client_jobs:)
}

fn handle_delete(
  state: QueueState,
  id: Int,
  reply: Subject(Response),
) -> actor.Next(QueueState, QueueMessage) {
  case dict.get(state.jobs, id) {
    Error(_) -> {
      process.send(reply, NoJob)
      actor.continue(state)
    }
    Ok(_) -> {
      let state = remove_job(state, id)
      process.send(reply, OkResponse)
      actor.continue(state)
    }
  }
}

fn remove_job(state: QueueState, id: Int) -> QueueState {
  let jobs = dict.delete(state.jobs, id)
  let client_jobs = case dict.get(state.working, id) {
    Ok(cid) -> {
      let ids =
        dict.get(state.client_jobs, cid)
        |> result.unwrap([])
        |> list.filter(fn(jid) { jid != id })
      dict.insert(state.client_jobs, cid, ids)
    }
    Error(_) -> state.client_jobs
  }
  let working = dict.delete(state.working, id)
  QueueState(..state, jobs:, working:, client_jobs:)
}

fn handle_abort(
  state: QueueState,
  client_id: Int,
  id: Int,
  reply: Subject(Response),
) -> actor.Next(QueueState, QueueMessage) {
  case dict.get(state.jobs, id) {
    Error(_) -> {
      process.send(reply, NoJob)
      actor.continue(state)
    }
    Ok(job) -> {
      case dict.get(state.working, id) {
        Error(_) -> {
          process.send(reply, ErrorResponse("job is not being worked on"))
          actor.continue(state)
        }
        Ok(assigned_client) -> {
          use <- bool.lazy_guard(
            when: assigned_client != client_id,
            return: fn() {
              process.send(
                reply,
                ErrorResponse("job is assigned to another client"),
              )
              actor.continue(state)
            },
          )

          let state = unassign_job(state, id, client_id)
          process.send(reply, OkResponse)

          let #(state, matched) = try_match_waiters(state, job.queue, id)
          use <- bool.lazy_guard(when: matched, return: fn() {
            actor.continue(state)
          })

          let state = enqueue_job(state, job.queue, id, job.priority)
          actor.continue(state)
        }
      }
    }
  }
}

fn unassign_job(state: QueueState, job_id: Int, client_id: Int) -> QueueState {
  let working = dict.delete(state.working, job_id)
  let client_existing =
    dict.get(state.client_jobs, client_id) |> result.unwrap([])
  let client_jobs =
    dict.insert(
      state.client_jobs,
      client_id,
      list.filter(client_existing, fn(id) { id != job_id }),
    )
  QueueState(..state, working:, client_jobs:)
}

fn handle_disconnect(
  state: QueueState,
  client_id: Int,
) -> actor.Next(QueueState, QueueMessage) {
  let waiters = list.filter(state.waiters, fn(w) { w.client_id != client_id })
  let state = QueueState(..state, waiters:)

  let job_ids = dict.get(state.client_jobs, client_id) |> result.unwrap([])
  let state = requeue_jobs(state, client_id, job_ids)
  let client_jobs = dict.delete(state.client_jobs, client_id)
  actor.continue(QueueState(..state, client_jobs:))
}

fn requeue_jobs(
  state: QueueState,
  client_id: Int,
  job_ids: List(Int),
) -> QueueState {
  case job_ids {
    [] -> state
    [job_id, ..rest] -> {
      let state = case dict.get(state.jobs, job_id) {
        Error(_) -> state
        Ok(job) -> {
          let state = unassign_job(state, job_id, client_id)
          let #(state, matched) = try_match_waiters(state, job.queue, job_id)
          case matched {
            True -> state
            False -> enqueue_job(state, job.queue, job_id, job.priority)
          }
        }
      }
      requeue_jobs(state, client_id, rest)
    }
  }
}

fn try_match_waiters(
  state: QueueState,
  queue: String,
  job_id: Int,
) -> #(QueueState, Bool) {
  case find_matching_waiter(state.waiters, queue, []) {
    None -> #(state, False)
    Some(#(waiter, remaining_waiters)) -> {
      let assert Ok(job) = dict.get(state.jobs, job_id)
      let state =
        QueueState(..state, waiters: remaining_waiters)
        |> assign_job(job_id, job, waiter.client_id)
      process.send(
        waiter.reply,
        GetOk(
          id: job_id,
          job: job.data,
          priority: job.priority,
          queue: job.queue,
        ),
      )
      #(state, True)
    }
  }
}

fn find_matching_waiter(
  waiters: List(Waiter),
  queue: String,
  checked: List(Waiter),
) -> Option(#(Waiter, List(Waiter))) {
  case waiters {
    [] -> None
    [waiter, ..rest] -> {
      case list.contains(waiter.queues, queue) {
        True -> Some(#(waiter, list.append(list.reverse(checked), rest)))
        False -> find_matching_waiter(rest, queue, [waiter, ..checked])
      }
    }
  }
}

pub type ConnectionState {
  ConnectionState(
    buffer: String,
    client_id: Int,
    queue: Subject(QueueMessage),
    response_subject: Subject(Response),
  )
}

fn handle_connection(
  queue: Subject(QueueMessage),
  client_counter: Counter,
  conn: glisten.Connection(Response),
) -> #(ConnectionState, Option(process.Selector(Response))) {
  let assert Ok(glisten.ConnectionInfo(ip_address:, port:)) =
    glisten.get_client_info(conn)
  logging.log(
    logging.Debug,
    "New connection from "
      <> glisten.ip_address_to_string(ip_address)
      <> " on "
      <> int.to_string(port),
  )

  let client_id = next_id(client_counter)
  let response_subject = process.new_subject()
  let selector =
    process.new_selector()
    |> process.select(response_subject)

  let state = ConnectionState(buffer: "", client_id:, queue:, response_subject:)
  #(state, Some(selector))
}

fn handle_tcp_message(
  state: ConnectionState,
  msg: glisten.Message(Response),
  conn: glisten.Connection(Response),
) -> glisten.Next(ConnectionState, glisten.Message(Response)) {
  case msg {
    glisten.Packet(data) -> handle_packet(state, data)
    glisten.User(response) -> {
      let line = encode_response(response) <> "\n"
      let _ = glisten.send(conn, bytes_tree.from_string(line))
      glisten.continue(state)
    }
  }
}

fn handle_packet(
  state: ConnectionState,
  data: BitArray,
) -> glisten.Next(ConnectionState, glisten.Message(Response)) {
  case bit_array.to_string(data) {
    Error(_) -> glisten.stop()
    Ok(text) -> {
      let #(lines, new_buffer) = process_buffer(state.buffer, text)
      let state = ConnectionState(..state, buffer: new_buffer)
      handle_lines(state, lines)
    }
  }
}

fn handle_lines(
  state: ConnectionState,
  lines: List(String),
) -> glisten.Next(ConnectionState, glisten.Message(Response)) {
  case lines {
    [] -> glisten.continue(state)
    [line, ..rest] -> {
      case parse_request(line) {
        Error(_) ->
          process.send(
            state.queue,
            EchoReply(ErrorResponse("invalid request"), state.response_subject),
          )
        Ok(request) -> handle_request(state, request)
      }
      handle_lines(state, rest)
    }
  }
}

fn handle_request(state: ConnectionState, request: Request) -> Nil {
  let reply = state.response_subject
  case request {
    Put(queue:, job:, priority:) ->
      process.send(
        state.queue,
        PutJob(queue:, job_data: job, priority:, reply:),
      )
    Get(queues:, wait:) ->
      process.send(
        state.queue,
        GetJob(client_id: state.client_id, queues:, wait:, reply:),
      )
    Delete(id:) -> process.send(state.queue, DeleteJob(id:, reply:))
    Abort(id:) ->
      process.send(
        state.queue,
        AbortJob(client_id: state.client_id, id:, reply:),
      )
  }
}

fn handle_close(state: ConnectionState) -> Nil {
  process.send(state.queue, ClientDisconnect(client_id: state.client_id))
}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  let queue = start_queue()
  let client_counter = new_counter()
  let assert Ok(_) =
    glisten.new(handle_connection(queue, client_counter, _), handle_tcp_message)
    |> glisten.with_close(handle_close)
    |> glisten.with_pool_size(200)
    |> glisten.bind("::")
    |> glisten.start(3050)

  process.sleep_forever()
}
