use anyhow::Result;
use std::sync::{Mutex, MutexGuard};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

/// Lock a mutex and return a Result.
/// Finds poison errors and returns them as Anyhow errors.
pub fn lock_mutex<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))
}

/// Lock an async mutex and return the guard.
/// Tokio mutexes don't poison, so this just awaits.
pub async fn lock_async_mutex<T>(mutex: &AsyncMutex<T>) -> AsyncMutexGuard<'_, T> {
    mutex.lock().await
}
