import gleam/erlang/process
import gleam/option.{None, Some}
import problem10

pub fn is_valid_filename_basic_test() {
  assert problem10.is_valid_filename("/test.txt")
  assert problem10.is_valid_filename("/foo/bar.txt")
  assert problem10.is_valid_filename("/a/b/c/d")
}

pub fn is_valid_filename_must_start_with_slash_test() {
  assert !problem10.is_valid_filename("test.txt")
  assert !problem10.is_valid_filename("foo/bar")
}

pub fn is_valid_filename_just_slash_test() {
  assert problem10.is_valid_filename("/")
}

pub fn is_valid_filename_illegal_chars_test() {
  assert !problem10.is_valid_filename("/test file.txt")
  assert !problem10.is_valid_filename("/test@file")
  assert !problem10.is_valid_filename("/test#file")
  assert !problem10.is_valid_filename("/test!file")
  assert !problem10.is_valid_filename("/test[file]")
}

pub fn is_valid_filename_empty_test() {
  assert !problem10.is_valid_filename("")
}

pub fn is_valid_data_ascii_test() {
  assert problem10.is_valid_data(<<"hello world":utf8>>)
  assert problem10.is_valid_data(<<"line1\nline2":utf8>>)
  assert problem10.is_valid_data(<<"tab\there":utf8>>)
  assert problem10.is_valid_data(<<>>)
}

pub fn is_valid_data_non_ascii_test() {
  assert !problem10.is_valid_data(<<0x00>>)
  assert !problem10.is_valid_data(<<0x01>>)
  assert !problem10.is_valid_data(<<0x7F>>)
}

pub fn parse_command_help_test() {
  assert problem10.parse_command("help") == Ok(problem10.Help)
  assert problem10.parse_command("HELP") == Ok(problem10.Help)
  assert problem10.parse_command("Help") == Ok(problem10.Help)
}

pub fn parse_command_get_test() {
  assert problem10.parse_command("get /test.txt")
    == Ok(problem10.Get(file: "/test.txt", revision: None))
}

pub fn parse_command_get_with_revision_test() {
  assert problem10.parse_command("GET /test.txt r3")
    == Ok(problem10.Get(file: "/test.txt", revision: Some(3)))
}

pub fn parse_command_get_no_file_test() {
  assert problem10.parse_command("get") == Error("usage: GET file [revision]")
}

pub fn parse_command_put_test() {
  assert problem10.parse_command("put /test.txt 5")
    == Ok(problem10.Put(file: "/test.txt", length: 5))
}

pub fn parse_command_put_no_args_test() {
  assert problem10.parse_command("put")
    == Error("usage: PUT file length newline data")
}

pub fn parse_command_list_test() {
  assert problem10.parse_command("list /") == Ok(problem10.List(directory: "/"))
  assert problem10.parse_command("LIST /foo")
    == Ok(problem10.List(directory: "/foo"))
}

pub fn parse_command_list_no_dir_test() {
  assert problem10.parse_command("list") == Error("usage: LIST dir")
}

pub fn parse_command_unknown_test() {
  assert problem10.parse_command("delete /foo")
    == Error("illegal method: delete")
}

pub fn vcs_put_and_get_test() {
  let vcs = problem10.start_vcs()

  let rev =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/test.txt", data: <<"hello":utf8>>, reply:)
    })
  assert rev == Ok(1)

  let data =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/test.txt", revision: None, reply:)
    })
  assert data == Ok(<<"hello":utf8>>)
}

pub fn vcs_get_nonexistent_test() {
  let vcs = problem10.start_vcs()

  let data =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/nope.txt", revision: None, reply:)
    })
  assert data == Error("no such file")
}

pub fn vcs_get_specific_revision_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"v1":utf8>>, reply:)
    })

  let assert Ok(2) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"v2":utf8>>, reply:)
    })

  let data =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/f.txt", revision: Some(1), reply:)
    })
  assert data == Ok(<<"v1":utf8>>)

  let data2 =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/f.txt", revision: Some(2), reply:)
    })
  assert data2 == Ok(<<"v2":utf8>>)
}

pub fn vcs_put_dedup_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"same":utf8>>, reply:)
    })

  let rev =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"same":utf8>>, reply:)
    })
  assert rev == Ok(1)

  let rev2 =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"different":utf8>>, reply:)
    })
  assert rev2 == Ok(2)
}

pub fn vcs_list_empty_test() {
  let vcs = problem10.start_vcs()

  let entries =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/", reply:)
    })
  assert entries == []
}

pub fn vcs_list_files_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/a.txt", data: <<"a":utf8>>, reply:)
    })

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/b.txt", data: <<"b":utf8>>, reply:)
    })

  let entries =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/", reply:)
    })
  assert entries == ["a.txt r1", "b.txt r1"]
}

pub fn vcs_list_subdirectory_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/dir/sub/file.txt", data: <<"x":utf8>>, reply:)
    })

  let root =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/", reply:)
    })
  assert root == ["dir/ DIR"]

  let dir =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/dir", reply:)
    })
  assert dir == ["sub/ DIR"]

  let sub =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/dir/sub", reply:)
    })
  assert sub == ["file.txt r1"]
}

pub fn vcs_list_sorted_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/zebra.txt", data: <<"z":utf8>>, reply:)
    })

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/alpha.txt", data: <<"a":utf8>>, reply:)
    })

  let entries =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/", reply:)
    })
  assert entries == ["alpha.txt r1", "zebra.txt r1"]
}

pub fn vcs_list_nonexistent_directory_test() {
  let vcs = problem10.start_vcs()

  let entries =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/nope", reply:)
    })
  assert entries == []
}

pub fn vcs_get_revision_zero_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"hi":utf8>>, reply:)
    })

  let result =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/f.txt", revision: Some(0), reply:)
    })
  assert result == Error("no such file")
}

pub fn vcs_get_revision_out_of_range_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/f.txt", data: <<"hi":utf8>>, reply:)
    })

  let result =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/f.txt", revision: Some(99), reply:)
    })
  assert result == Error("no such file")
}

pub fn vcs_list_mixed_files_and_dirs_test() {
  let vcs = problem10.start_vcs()

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/readme.txt", data: <<"hi":utf8>>, reply:)
    })

  let assert Ok(1) =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(
        file: "/src/main.gleam",
        data: <<"code":utf8>>,
        reply:,
      )
    })

  let entries =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleList(directory: "/", reply:)
    })
  assert entries == ["readme.txt r1", "src/ DIR"]
}

pub fn parse_command_case_insensitive_test() {
  assert problem10.parse_command("GET /f")
    == Ok(problem10.Get(file: "/f", revision: None))
  assert problem10.parse_command("gEt /f")
    == Ok(problem10.Get(file: "/f", revision: None))
  assert problem10.parse_command("PUT /f 5")
    == Ok(problem10.Put(file: "/f", length: 5))
  assert problem10.parse_command("LIST /") == Ok(problem10.List(directory: "/"))
}

pub fn vcs_put_empty_data_test() {
  let vcs = problem10.start_vcs()

  let rev =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandlePut(file: "/empty.txt", data: <<>>, reply:)
    })
  assert rev == Ok(1)

  let data =
    process.call(vcs, 1000, fn(reply) {
      problem10.HandleGet(file: "/empty.txt", revision: None, reply:)
    })
  assert data == Ok(<<>>)
}
