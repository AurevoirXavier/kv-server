extern crate kv_server;

// --- std ---
use std::{
    io::{self, Read},
    sync::Arc,
    thread::spawn,
};
// --- external ---
use futures::{sync::oneshot, Future};
use grpcio::{Environment, ServerBuilder};
// --- custom ---
use kv_server::{
    create_kv_server,
    hash::{MergePolicy, Options},
    HashEngineBuilder, Server,
};

fn main() {
    let service = create_kv_server(Server::new(
        HashEngineBuilder::new()
            .storage_dir("tests/data/test-grpc")
            .options(Options {
                //            file_size_limit: 2,
                file_size_limit: 5 * 0x100000,
                keep_old_files: false,
                merge_policy: MergePolicy::Test,
            })
            .build()
            .unwrap(),
    ));
    let mut server = ServerBuilder::new(Arc::new(Environment::new(1)))
        .register_service(service)
        .bind("127.0.0.1", 23333)
        .build()
        .unwrap();

    server.start();

    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }

    let (tx, rx) = oneshot::channel();
    spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });

    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
