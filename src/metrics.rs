use std::time::Instant;
use sysinfo::{Networks, ProcessesToUpdate, System};

#[derive(Debug, Clone, Default)]
pub struct HostMetricsSnapshot {
    pub disk_read_bytes_sec: u64,
    pub disk_write_bytes_sec: u64,
    pub net_tx_bytes_sec: u64,
    pub net_rx_bytes_sec: u64,
    pub timestamp: Option<Instant>,
}

pub struct MetricsCollector {
    sys: System,
    networks: Networks,
    last_snapshot: HostMetricsSnapshot,
    last_update: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        let mut networks = Networks::new_with_refreshed_list();
        
        // Initial refresh to establish baseline
        sys.refresh_cpu_all();
        sys.refresh_memory();
        sys.refresh_processes(ProcessesToUpdate::All, true);
        networks.refresh();
        
        Self {
            sys,
            networks,
            last_snapshot: HostMetricsSnapshot::default(),
            last_update: Instant::now(),
        }
    }

    pub fn refresh(&mut self) -> HostMetricsSnapshot {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        
        // Avoid division by zero or extremely fast refreshes
        if elapsed < 0.1 {
            return self.last_snapshot.clone();
        }

        // 1. Refresh Networks
        self.networks.refresh();
        
        // 2. Refresh Processes (for disk usage)
        self.sys.refresh_processes(ProcessesToUpdate::All, true);

        let mut total_rx = 0;
        let mut total_tx = 0;

        for (_interface_name, data) in &self.networks {
            total_rx += data.received();
            total_tx += data.transmitted();
        }

        let mut total_disk_read = 0;
        let mut total_disk_write = 0;

        for (_pid, process) in self.sys.processes() {
            let disk_usage = process.disk_usage();
            total_disk_read += disk_usage.read_bytes;
            total_disk_write += disk_usage.written_bytes;
        }

        // sysinfo process.disk_usage() returns "bytes read/written since start" or "during last interval"?
        // documentation says: "Returns the disk usage of this process." which contains total_read_bytes 
        // and written_bytes... wait.
        // Actually for `sysinfo`, `disk_usage()` usually returns the usage *since last refresh*.
        // Just to be safe, we'll verify behavior or treat them as rates if they are per-interval.
        // Looking at sysinfo docs: "read_bytes: Total bytes read... wait, old sysinfo had it differently."
        // IN sysinfo 0.30+: ProcessDiskUsage has `read_bytes` and `written_bytes` which are 
        // "Number of bytes read/written during the last interval." (if supported).
        // So these ARE rates (bytes per interval), which we then largely treat as "bytes in this last slice".
        // To get per-second rate, we divide by elapsed time?
        // Actually, typically sysinfo metrics like this are "amount since last refresh".
        // So Rate = Amount / Elapsed.
        
        // However, `data.received()` on NetworkData: "Returns the number of bytes received since the last refresh."
        // So yes, we need to divide by elapsed to get bytes/sec.

        // Wait, ProcessDiskUsage struct in 0.30:
        // read_bytes: "Bytes read since last update."
        // So yes, both are "since last update".

        let rx_rate = (total_rx as f64 / elapsed) as u64;
        let tx_rate = (total_tx as f64 / elapsed) as u64;
        let dist_read_rate = (total_disk_read as f64 / elapsed) as u64;
        let disk_write_rate = (total_disk_write as f64 / elapsed) as u64;

        self.last_snapshot = HostMetricsSnapshot {
            disk_read_bytes_sec: dist_read_rate,
            disk_write_bytes_sec: disk_write_rate,
            net_tx_bytes_sec: tx_rate,
            net_rx_bytes_sec: rx_rate,
            timestamp: Some(now),
        };
        
        self.last_update = now;
        self.last_snapshot.clone()
    }
}
