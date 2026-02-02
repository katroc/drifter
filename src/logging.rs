use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, Registry, layer::SubscriberExt, reload, util::SubscriberInitExt,
};

pub type LogHandle = reload::Handle<EnvFilter, Registry>;

pub fn init() -> (WorkerGuard, LogHandle) {
    let file_appender = tracing_appender::rolling::never(".", "debug.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .compact();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let (filter_layer, reload_handle) = reload::Layer::new(filter);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(file_layer)
        .init();

    (guard, reload_handle)
}
