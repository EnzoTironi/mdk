//! Exercise the released CLI's privacy-safe startup diagnostic.

#[cfg(unix)]
#[test]
fn socket_path_too_long_cli_reports_actionable_error_without_private_path() {
    use std::os::unix::ffi::OsStrExt;

    let root = tempfile::tempdir_in("/tmp").unwrap();
    let probe = root.path().join("x").join("wn-agent.sock");
    let overflow = root.path().join("x".repeat(200)).join("wn-agent.sock");
    let limit = fs_private::validate_private_unix_socket_path(&overflow)
        .unwrap_err()
        .max_path_bytes;
    // The final address fits exactly even if the child has a different PID width.
    // Only the private staging overhead makes startup fail.
    let parent_len = 1 + limit - probe.as_os_str().as_bytes().len();
    let socket = root
        .path()
        .join("x".repeat(parent_len))
        .join("wn-agent.sock");
    assert_eq!(socket.as_os_str().as_bytes().len(), limit);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_wn-agent"))
        .arg("--home")
        .arg(root.path())
        .arg("--socket")
        .arg(&socket)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("code=socket_path_too_long"));
    assert!(stderr.contains("shorten --home or --socket"));
    assert!(!stderr.contains(root.path().to_str().unwrap()));
    assert!(!socket.parent().unwrap().exists());
}
