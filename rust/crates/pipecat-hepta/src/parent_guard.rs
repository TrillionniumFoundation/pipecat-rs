//! Linux lifetime binding for the native media child, before it opens resources.
//!
//! This binds one process to its actual spawning parent. It grants no device,
//! conversation, filesystem or signing authority. It does not cover descendants,
//! roll back submitted audio, or promise that an uninterruptible kernel operation
//! has completed when a kill signal has merely been requested.
use std::io;

/// Arm Linux SIGKILL on parent death and verify the expected parent on both sides
/// of the operation. Call from the native executable's initial thread, before
/// creating its runtime, loading native libraries or opening any device.
/// The parent thread which creates this child must remain alive until the child
/// exits: Linux delivers this signal on that thread's death, even when other
/// threads in the parent process are still alive. The actual endpoint spawns
/// from its supervising initial thread and keeps it alive for that lifetime.
///
/// The parent passes its real PID through its private launch arguments. That PID
/// is a lifetime correlation, never an authentication credential. Checking again
/// after arming closes the race in which the parent dies before PR_SET_PDEATHSIG.
/// Any error must terminate startup; callers must not continue opening resources.
/// Subsequent credential changes or code that clears PR_SET_PDEATHSIG invalidate
/// this binding and are outside this native endpoint's supported lifecycle.
#[cfg(target_os = "linux")]
pub fn bind_native_parent(expected_parent_pid: u32) -> io::Result<()> {
    use rustix::process::{
        getppid, parent_process_death_signal, set_parent_process_death_signal, Pid, Signal,
    };
    let expected = i32::try_from(expected_parent_pid)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid native parent PID"))?;
    if getppid() != Some(expected) {
        return Err(io::Error::other(
            "native parent identity changed before binding",
        ));
    }
    set_parent_process_death_signal(Some(Signal::KILL))?;
    if getppid() != Some(expected) {
        return Err(io::Error::other(
            "native parent identity changed while binding",
        ));
    }
    if parent_process_death_signal()? != Some(Signal::KILL) {
        return Err(io::Error::other(
            "native parent death signal was not retained",
        ));
    }
    Ok(())
}

/// Other operating systems need their own verified process containment. A no-op
/// would incorrectly let an unprotected child advertise the Linux guarantee.
#[cfg(not(target_os = "linux"))]
pub fn bind_native_parent(_expected_parent_pid: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native parent death binding is implemented only on Linux",
    ))
}
