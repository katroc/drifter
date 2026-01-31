use std::time::Instant;
use sysinfo::{Networks, ProcessesToUpdate, System};

#[derive(Debug, Clone, Default)]
pub struct HostMetricsSnapshot {
    pub disk_read_bytes_sec: u64,
    pub disk_write_bytes_sec: u64,
    pub net_tx_bytes_sec: u64,
    pub net_rx_bytes_sec: u64,

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

        for process in self.sys.processes().values() {
            let disk_usage = process.disk_usage();
            total_disk_read += disk_usage.read_bytes;
            total_disk_write += disk_usage.written_bytes;
        }

        let rx_rate = (total_rx as f64 / elapsed) as u64;
        let tx_rate = (total_tx as f64 / elapsed) as u64;
        let disk_read_rate = (total_disk_read as f64 / elapsed) as u64;
        let disk_write_rate = (total_disk_write as f64 / elapsed) as u64;

        self.last_snapshot = HostMetricsSnapshot {
            disk_read_bytes_sec: disk_read_rate,
            disk_write_bytes_sec: disk_write_rate,
            net_tx_bytes_sec: tx_rate,
            net_rx_bytes_sec: rx_rate,
        };
        
        self.last_update = now;
        self.last_snapshot.clone()
    }
}
