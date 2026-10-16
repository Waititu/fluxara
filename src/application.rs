use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub status: &'static str,
    pub service: &'static str,
    pub version: &'static str,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HealthService;

impl HealthService {
    pub fn snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            status: "ok",
            service: "fluxara",
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HealthService;

    #[test]
    fn health_snapshot_identifies_the_service() {
        let snapshot = HealthService.snapshot();

        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.service, "fluxara");
        assert_eq!(snapshot.version, env!("CARGO_PKG_VERSION"));
    }
}
