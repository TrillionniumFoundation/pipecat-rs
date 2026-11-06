//! Actual Linux process death and resource-release checks. No audio device,
//! model, production owner, credentials or application service is opened here.
#![cfg(all(target_os = "linux", feature = "process-supervision"))]
use pipecat_hepta::parent_guard::bind_native_parent;
use rustix::process::{
    getppid, kill_process, parent_process_death_signal, set_child_subreaper, waitpid, Pid, Signal,
    WaitOptions, WaitStatus,
};
use serde_json::{json, Value};
use std::{
    fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const ROLE: &str = "PIPECAT_PARENT_GUARD_TEST_ROLE";
const CASE: &str = "PIPECAT_PARENT_GUARD_TEST_CASE";
const ROOT: &str = "PIPECAT_PARENT_GUARD_TEST_ROOT";
const EXPECTED_PARENT: &str = "PIPECAT_PARENT_GUARD_TEST_PARENT";

#[derive(Clone)]
struct ProcessIdentity {
    pid: u32,
    start_time: u64,
}
impl ProcessIdentity {
    fn read(pid: u32) -> io::Result<Self> {
        let text = fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let fields: Vec<_> = text
            .rsplit_once(')')
            .ok_or_else(|| io::Error::other("invalid proc stat"))?
            .1
            .split_whitespace()
            .collect();
        let start_time = fields
            .get(19)
            .ok_or_else(|| io::Error::other("missing process start identity"))?
            .parse()
            .map_err(io::Error::other)?;
        Ok(Self { pid, start_time })
    }
    fn still_matches(&self) -> bool {
        Self::read(self.pid).is_ok_and(|current| current.start_time == self.start_time)
    }
}

struct OwnedSupervisor {
    child: Child,
    native: Option<ProcessIdentity>,
}
impl OwnedSupervisor {
    fn kill_and_wait(&mut self) -> ExitStatus {
        self.child.kill().unwrap();
        self.child.wait().unwrap()
    }
}
impl Drop for OwnedSupervisor {
    fn drop(&mut self) {
        // Only our exact spawned process and the recorded native incarnation.
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(native) = &self.native {
            if native.still_matches() {
                if let Some(pid) = Pid::from_raw(native.pid as i32) {
                    let _ = kill_process(pid, Signal::KILL);
                    let _ = reap_native(pid);
                }
            }
        }
    }
}

fn helper(role: &str, case: &str, root: &Path) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "parent_guard_helper",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ROLE, role)
        .env(CASE, case)
        .env(ROOT, root)
        .env(EXPECTED_PARENT, std::process::id().to_string())
        .stdin(Stdio::null());
    command
}
fn publish(root: &Path, name: &str, value: Value) {
    let temporary = root.join(format!("{name}.tmp"));
    fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    fs::rename(temporary, root.join(name)).unwrap();
}
fn read_marker(root: &Path, name: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match fs::read(root.join(name)) {
            Ok(bytes) => return serde_json::from_slice(&bytes).unwrap(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => panic!("marker {name}: {error}"),
        }
        assert!(Instant::now() < deadline, "marker {name} was not published");
        thread::sleep(Duration::from_millis(5));
    }
}
fn reap_native(pid: Pid) -> Result<WaitStatus, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(Some(pid), WaitOptions::NOHANG) {
            Ok(Some((found, status))) if found == pid => return Ok(status),
            Ok(Some(_)) => return Err("waitpid returned a different child".into()),
            Ok(None) => {}
            Err(error) => return Err(format!("native waitpid: {error}")),
        }
        if Instant::now() >= deadline {
            return Err("native process did not exit after parent death".into());
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn run_fixture(case: &str) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "pipecat-parent-guard-{}-{case}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let log = fs::File::create(root.join("fixture.log")).unwrap();
    let mut child = helper("scenario", case, &root)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let text = fs::read_to_string(root.join("fixture.log")).unwrap_or_default();
            panic!("parent guard fixture {case} timed out: {text}");
        }
        thread::sleep(Duration::from_millis(10));
    };
    let text = fs::read_to_string(root.join("fixture.log")).unwrap();
    assert!(status.success(), "parent guard fixture {case}: {text}");
    println!("{text}");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_or_mismatched_parent_is_rejected_before_binding() {
    run_fixture("validate");
}

#[test]
fn supervisor_sigkill_kills_reaps_native_and_releases_listener() {
    run_fixture("death");
}

#[test]
fn parent_lost_before_arming_is_rejected_before_resource_open() {
    run_fixture("stale");
}

// The real process fixtures run this one entry point explicitly. The isolated
// scenario is a subreaper so its native child can be waited by exact PID without
// changing the test runner's child ownership or reaping any unrelated process.
#[test]
#[ignore = "subprocess fixture; invoked by the three process tests"]
fn parent_guard_helper() {
    let role = std::env::var(ROLE).expect("explicit fixture role required");
    let case = std::env::var(CASE).unwrap();
    let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
    let expected: u32 = std::env::var(EXPECTED_PARENT).unwrap().parse().unwrap();
    match role.as_str() {
        "scenario" => {
            if case == "validate" {
                let before = parent_process_death_signal().unwrap();
                for invalid in [0, u32::MAX, std::process::id()] {
                    assert!(bind_native_parent(invalid).is_err());
                    assert_eq!(parent_process_death_signal().unwrap(), before);
                }
                bind_native_parent(expected).unwrap();
                assert_eq!(getppid().unwrap().as_raw_nonzero().get() as u32, expected);
                assert_eq!(parent_process_death_signal().unwrap(), Some(Signal::KILL));
                println!(
                    "{}",
                    json!({"case":case,"invalid_rejected":3,"valid_parent":expected,"signal":"SIGKILL"})
                );
                return;
            }
            bind_native_parent(expected).unwrap();
            // The kernel takes a nonzero boolean; rustix represents it as Pid.
            set_child_subreaper(Some(Pid::INIT)).unwrap();
            let child = helper("supervisor", &case, &root)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let mut owned = OwnedSupervisor {
                child,
                native: None,
            };
            let spawned = read_marker(&root, "spawned.json");
            let native_pid = spawned["native_pid"].as_u64().unwrap() as u32;
            let identity = ProcessIdentity::read(native_pid).unwrap();
            owned.native = Some(identity.clone());
            let native = Pid::from_raw(native_pid as i32).unwrap();
            let ready = read_marker(
                &root,
                if case == "stale" {
                    "before-arm.json"
                } else {
                    "ready.json"
                },
            );
            assert_eq!(ready["pid"], native_pid);
            assert_eq!(ready["parent_pid"], owned.child.id());
            assert_eq!(ready["start_time"], identity.start_time);
            assert!(identity.still_matches());
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(owned.kill_and_wait().signal(), Some(9));
            if case == "stale" {
                publish(&root, "continue.json", json!({"continue":true}));
            }
            let status = reap_native(native).unwrap();
            owned.native = None;
            if case == "death" {
                assert_eq!(status.terminating_signal(), Some(9));
                let address = ready["listener"].as_str().unwrap();
                let rebound = TcpListener::bind(address)
                    .expect("native listening socket survived process exit");
                drop(rebound);
                println!(
                    "{}",
                    json!({"case":case,"native_pid":native_pid,"start_time":identity.start_time,"native_signal":9,"native_reaped":true,"listener_released":true})
                );
            } else {
                assert_eq!(status.exit_status(), Some(0));
                let rejected = read_marker(&root, "rejected.json");
                assert!(rejected["error"]
                    .as_str()
                    .unwrap()
                    .contains("parent identity"));
                assert!(!root.join("ready.json").exists());
                println!(
                    "{}",
                    json!({"case":case,"native_pid":native_pid,"native_reaped":true,"stale_parent_rejected":true,"resource_opened":false})
                );
            }
            assert!(!identity.still_matches());
            set_child_subreaper(None).unwrap();
        }
        "supervisor" => {
            bind_native_parent(expected).unwrap();
            let mut native = helper("native", &case, &root)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            publish(&root, "spawned.json", json!({"native_pid":native.id()}));
            let status = native.wait().unwrap();
            panic!("native exited before the parent-death scenario: {status}");
        }
        "native" => {
            let identity = ProcessIdentity::read(std::process::id()).unwrap();
            if case == "stale" {
                publish(
                    &root,
                    "before-arm.json",
                    json!({"pid":identity.pid,"start_time":identity.start_time,"parent_pid":expected}),
                );
                read_marker(&root, "continue.json");
                let error = bind_native_parent(expected).unwrap_err();
                publish(&root, "rejected.json", json!({"error":error.to_string()}));
                return;
            }
            bind_native_parent(expected).unwrap();
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            publish(
                &root,
                "ready.json",
                json!({"pid":identity.pid,"start_time":identity.start_time,"parent_pid":expected,"listener":listener.local_addr().unwrap().to_string()}),
            );
            // A blocking native-style wait never polls a Tokio cancellation token.
            loop {
                thread::park();
            }
        }
        _ => panic!("unknown fixture role"),
    }
}
