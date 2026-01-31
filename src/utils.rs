use std::sync::{Mutex, MutexGuard};
use anyhow::Result;

/// Lock a mutex and return a Result.
/// Finds poison errors and returns them as Anyhow errors.
pub fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex.lock().map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))
}
