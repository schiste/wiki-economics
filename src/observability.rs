use std::fs;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemorySnapshot {
    pub rss_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub cgroup_peak_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
}

impl MemorySnapshot {
    pub fn capture() -> Self {
        Self {
            rss_bytes: fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|status| parse_proc_status_rss(&status)),
            cgroup_current_bytes: read_byte_counter("/sys/fs/cgroup/memory.current"),
            cgroup_peak_bytes: read_byte_counter("/sys/fs/cgroup/memory.peak"),
            cgroup_limit_bytes: read_byte_counter("/sys/fs/cgroup/memory.max"),
        }
    }
}

fn read_byte_counter(path: &str) -> Option<u64> {
    fs::read_to_string(path)
        .ok()
        .and_then(|value| parse_byte_counter(&value))
}

fn parse_byte_counter(value: &str) -> Option<u64> {
    value.trim().parse().ok()
}

fn parse_proc_status_rss(status: &str) -> Option<u64> {
    let kib = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
    })?;
    kib.checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_rss_in_kibibytes() {
        assert_eq!(
            parse_proc_status_rss("Name:\twiki-econ\nVmRSS:\t42 kB\n"),
            Some(43_008)
        );
        assert_eq!(parse_proc_status_rss("Name:\twiki-econ\n"), None);
        assert_eq!(parse_proc_status_rss("VmRSS:\tnot-a-number kB\n"), None);
    }

    #[test]
    fn parses_finite_cgroup_counters_only() {
        assert_eq!(parse_byte_counter("6291456\n"), Some(6_291_456));
        assert_eq!(parse_byte_counter("max\n"), None);
    }

    #[test]
    fn captures_available_process_counters() {
        let snapshot = MemorySnapshot::capture();
        #[cfg(target_os = "linux")]
        assert!(snapshot.rss_bytes.is_some());
        #[cfg(not(target_os = "linux"))]
        assert_eq!(snapshot, MemorySnapshot::default());
    }
}
