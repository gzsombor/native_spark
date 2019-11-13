use super::*;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::net::{Ipv4Addr, SocketAddr};
//use std::io::prelude::*;
//use std::io::prelude::*;
//use std::io::Write;
use std::sync::Arc;

pub struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    the_cache: BoundedMemoryCache,
    pub cache_tracker: CacheTracker,
}

impl Env {
    pub fn new(is_master_process: bool, master_addr: SocketAddr) -> Self {
        let cache = BoundedMemoryCache::new();
        Env {
            map_output_tracker: MapOutputTracker::new(is_master_process, master_addr),
            shuffle_manager: ShuffleManager::new(),
            shuffle_fetcher: ShuffleFetcher,
            the_cache: cache,
            cache_tracker: CacheTracker::new(is_master_process, master_addr, cache.get_capacity()),
        }
    }
}

lazy_static! {
    pub static ref shuffle_cache: Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    // Too lazy to choose a proper logger. Currently using a static log file to log the whole process. Just a crude version of logger.
    pub static ref is_master: bool = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        match args.get(0).as_ref().map(|arg| &arg[..]) {
            Some("slave") => false,
            _ => true,
        }
    };
    pub static ref hosts: Hosts = Hosts::load().unwrap();
    pub static ref env: Env = Env::new(*is_master, hosts.master);

    pub static ref local_ip: Ipv4Addr = std::env::var("SPARK_LOCAL_IP")
        .expect("You must set the SPARK_LOCAL_IP environment variable")
        .parse()
        .unwrap();
}

impl <'a> Default for Env {
    fn default() -> Env {
        Env::new(*is_master, hosts.master)
    }
}

