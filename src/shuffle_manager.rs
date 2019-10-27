use super::*;
//use actix_files as fserver;
use actix_web::HttpServer;
use actix_web::{
    get,
    web::{Bytes, Path, Data},
    App,
};
use rand::Rng;
use std::collections::HashMap;
use std::fs;
use std::thread;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

type ShuffleCacheType = Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>>;

// creates directories and files required for storing shuffle data.  It also creates the file server required for serving files via http request
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ShuffleManager {
    local_dir: String,
    shuffle_dir: String,
    server_uri: String,
    server_address: String,
    shuffle_cache: ShuffleCacheType,
}

impl ShuffleManager {
    pub fn new() -> Self {
        //TODO replace all hardcoded values with environment variables
        let local_dir_root = "/tmp";
        let mut tries = 0;
        let mut found_local_dir = false;
        let mut local_dir = String::new();
        let mut local_dir_uuid = String::new();
        //TODO error logging
        while (!found_local_dir) && (tries < 10) {
            tries += 1;
            let uuid = Uuid::new_v4();
            local_dir_uuid = uuid.to_string();
            local_dir = format!("{}/spark-local-{}", local_dir_root, local_dir_uuid);
            let path = std::path::Path::new(&local_dir);
            if !path.exists() {
                info!("creating directory at path {:?} loc {:?}", path, local_dir);
                fs::create_dir_all(path);
                found_local_dir = true;
            }
        }
        if !found_local_dir {
            panic!(
                "failed 10 attempts to create local dir in {}",
                local_dir_root
            );
        }
        let shuffle_dir = format!("{}/shuffle", local_dir);
        fs::create_dir_all(shuffle_dir.clone());

        // for experimenting this should not lead to any clashes
        let port = 5000 + rand::thread_rng().gen_range(0, 1000);
        let server_uri = format!(
            "http://{}:{}",
            env::local_ip.clone(),
            port,
            //            local_dir_uuid
        );
        info!("server_uri {:?}", server_uri);
        let server_address = format!("{}:{}", env::local_ip.clone(), port);
        info!("server_address {:?}", server_address);
        let relative_path = format!("/spark-local-{}", local_dir_uuid);
        let local_dir_clone = local_dir.clone();
        info!("relative path {}", relative_path);
        info!("local_dir path {}", local_dir);
        info!("shuffle dir path {}", shuffle_dir);
        let shuffle_cache = Arc::new(RwLock::new(HashMap::new()));

        let s = ShuffleManager {
            local_dir,
            shuffle_dir,
            server_uri,
            server_address,
            shuffle_cache,
        };
        info!("shuffle manager inside new {:?}", s);
        s
    }

    pub fn start_server(&self) {
        let http_server_shuffle_cache = Data::new(&self.shuffle_cache);
        let server_address_clone = self.server_address.clone();
        thread::spawn(move || {

            #[get("/shuffle/{shuffleid}/{inputid}/{reduceid}")]
            fn get_shuffle_data((info,cache): (Path<(usize, usize, usize)>, Data<ShuffleCacheType>)) -> Bytes {
                //                println!("inside get shuffle data in  actix server");
                //                println!(
                //                    "bytes inside server {}",
                //                    env::shuffle_cache
                //                        .read()
                //                        .unwrap()
                //                        .get(&(info.0, info.1, info.2))
                //                        .unwrap()
                //                        .clone()[0]
                //                );
                Bytes::from(
                    cache
                        .read()
                        .unwrap()
                        .get(&(info.0, info.1, info.2))
                        .unwrap()[..],
                )
            }
            info!("starting server for shuffle task");
            #[get("/")]
            fn no_params() -> &'static str {
                "Hello world!\r"
            }
            match HttpServer::new(move || {
                App::new()
                    .register_data(http_server_shuffle_cache)
                    .service(get_shuffle_data)
                    .service(no_params)
                //                    .service(
                //                        // static files
                //                        fserver::Files::new(&relative_path, &local_dir_clone),
                //                    )
            })
            .workers(8)
            .bind(server_address_clone)
            {
                Ok(s) => {
                    info!("server for shufflemap task binded");
                    match s.run() {
                        Ok(_) => {
                            info!("server for shufflemap task started");
                        }
                        Err(e) => {
                            info!("cannot start server for shufflemap task started {}", e);
                        }
                    }
                }
                Err(e) => {
                    info!("cannot bind server for shuffle map task {}", e);
                    std::process::exit(0)
                }
            }
        });
    }

    pub fn get_server_uri(&self) -> String {
        self.server_uri.clone()
    }

    pub fn get_output_file(&self, shuffle_id: usize, input_id: usize, output_id: usize) -> String {
        let path = format!("{}/{}/{}", self.shuffle_dir, shuffle_id, input_id);
        fs::create_dir_all(&path);
        let file_path = format!("{}/{}", path, output_id);
        fs::File::create(file_path.clone());
        file_path
    }

    pub fn put_shuffle_cache(&self, shuffle_id: usize, partition: usize, idx: usize, ser_bytes: Vec<u8>) {
        self.shuffle_cache
            .write()
            .unwrap()
            .insert((shuffle_id, partition, idx), ser_bytes);
    }
}

//TODO implement drop for deleting files created when the shuffle manager stops
