use std::collections::BTreeSet;
use std::net::SocketAddr;

#[derive(Debug, Clone, Default)]
pub enum Discovery {
    #[default]
    None,
    Static(Vec<SocketAddr>),
    Dns {
        name: String,
        port: u16,
    },
    Kubernetes {
        service: String,
        namespace: String,
        port: u16,
    },
    #[cfg(feature = "mdns")]
    Mdns {
        service: String,
    },
}

impl Discovery {
    pub fn dns_name(&self) -> Option<String> {
        match self {
            Discovery::Dns { name, .. } => Some(name.clone()),
            Discovery::Kubernetes { service, namespace, .. } => {
                Some(format!("{service}.{namespace}.svc.cluster.local"))
            }
            _ => None,
        }
    }

    pub async fn resolve(&self) -> Vec<SocketAddr> {
        let found: BTreeSet<SocketAddr> = match self {
            Discovery::None => BTreeSet::new(),
            Discovery::Static(addrs) => addrs.iter().copied().collect(),
            Discovery::Dns { port, .. } | Discovery::Kubernetes { port, .. } => {
                let name = self.dns_name().unwrap_or_default();
                match tokio::net::lookup_host((name.as_str(), *port)).await {
                    Ok(addrs) => addrs.collect(),
                    Err(_) => BTreeSet::new(),
                }
            }
            #[cfg(feature = "mdns")]
            Discovery::Mdns { service } => mdns::browse(service),
        };
        found.into_iter().collect()
    }
}

#[cfg(feature = "mdns")]
pub use mdns::{Announcement, announce_mdns};

#[cfg(feature = "mdns")]
mod mdns {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex, OnceLock};

    use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};

    type Found = Arc<Mutex<BTreeMap<String, SocketAddr>>>;

    struct Registry {
        daemon: ServiceDaemon,
        browsing: Mutex<HashMap<String, Found>>,
    }

    fn registry() -> std::io::Result<&'static Registry> {
        static REGISTRY: OnceLock<Option<Registry>> = OnceLock::new();
        REGISTRY
            .get_or_init(|| ServiceDaemon::new().ok().map(|daemon| Registry { daemon, browsing: Mutex::default() }))
            .as_ref()
            .ok_or_else(|| std::io::Error::other("mDNS is unavailable on this host"))
    }

    fn service_type(service: &str) -> String {
        format!("_{service}._tcp.local.")
    }

    pub struct Announcement {
        fullname: String,
    }

    impl Drop for Announcement {
        fn drop(&mut self) {
            if let Ok(r) = registry() {
                let _ = r.daemon.unregister(&self.fullname);
            }
        }
    }

    pub fn announce_mdns(service: &str, id: u64, addr: SocketAddr) -> std::io::Result<Announcement> {
        let r = registry()?;
        let properties = [("addr", addr.to_string())];
        let info = ServiceInfo::new(
            &service_type(service),
            &format!("node-{id}"),
            &format!("quorium-{id}.local."),
            addr.ip(),
            addr.port(),
            &properties[..],
        )
        .map_err(std::io::Error::other)?;
        let fullname = info.get_fullname().to_string();
        r.daemon.register(info).map_err(std::io::Error::other)?;
        Ok(Announcement { fullname })
    }

    pub(super) fn browse(service: &str) -> BTreeSet<SocketAddr> {
        let Ok(r) = registry() else { return BTreeSet::new() };
        let ty = service_type(service);
        let found = {
            let mut browsing = r.browsing.lock().unwrap();
            if let Some(found) = browsing.get(&ty) {
                found.clone()
            } else {
                let found: Found = Arc::default();
                let Ok(events) = r.daemon.browse(&ty) else { return BTreeSet::new() };
                let sink = found.clone();
                std::thread::spawn(move || {
                    while let Ok(event) = events.recv() {
                        match event {
                            ServiceEvent::ServiceResolved(info) => {
                                let addr =
                                    info.get_property_val_str("addr").and_then(|a| a.parse().ok()).or_else(|| {
                                        info.get_addresses_v4()
                                            .into_iter()
                                            .next()
                                            .map(|ip| SocketAddr::from((ip, info.get_port())))
                                    });
                                if let Some(addr) = addr {
                                    sink.lock().unwrap().insert(info.get_fullname().to_string(), addr);
                                }
                            }
                            ServiceEvent::ServiceRemoved(_, fullname) => {
                                sink.lock().unwrap().remove(&fullname);
                            }
                            _ => {}
                        }
                    }
                });
                browsing.insert(ty, found.clone());
                found
            }
        };
        let found = found.lock().unwrap();
        found.values().copied().collect()
    }
}
