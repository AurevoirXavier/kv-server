extern crate kv_server;

// --- std ---

use std::{
    io,
    fs::{read_dir, remove_dir_all},
    sync::Arc,
};
// --- external ---
use futures::{Stream, Future};
use hashbrown::HashMap;
// --- custom ---
use kv_server::{
    HashEngine, HashEngineBuilder, HashScanner, Scanner, Server,
    hash::{MergePolicy, Options},
    protos::{
        kv_server::{Operation, Request, ScanRequest, Status},
        kv_server_grpc::KvServerClient,
    },
};

const STORAGE_DIR: &'static str = "tests/data/test-all";

fn new_server(options: Options) -> Server<HashEngine> {
    let _ = remove_dir_all(STORAGE_DIR);

    Server::new(HashEngineBuilder::new()
        .storage_dir(STORAGE_DIR)
        .options(options)
        .build()
        .unwrap())
}

#[test]
fn del() {
    const N: u8 = 5;

    let mut server = new_server(Options {
        file_size_limit: 200,
        keep_old_files: true,
        merge_policy: MergePolicy::Test,
    });

    for i in 0..N { server.put(vec![i; 8], vec![i; 256]).unwrap(); }
    for i in 0..N { server.del(&vec![i; 8]).unwrap(); }
    for i in 0..N { assert_eq!(server.get(&vec![i; 8]).unwrap(), None); }
}

#[test]
fn scan() {
    const N: u8 = 100;

    let mut server = new_server(Options {
        file_size_limit: 5 * 0x100000,
        keep_old_files: true,
        merge_policy: MergePolicy::Test,
    });

    for i in 0..N { server.put(vec![i; 8], vec![i; 256]).unwrap(); }

    let scanner = Scanner::HashScanner(HashScanner {
        range: 20,
        regex: None,
    });
    let kvs = server.scan(scanner).unwrap().1;
    assert_eq!(kvs.len(), 20);

    let scanner = Scanner::HashScanner(HashScanner {
        range: -1,
        regex: None,
    });
    let kvs = server.scan(scanner).unwrap().1;
    assert_eq!(kvs.len(), 100);

    for i in 0..N { server.del(&vec![i; 8]).unwrap(); }

    let scanner = Scanner::HashScanner(HashScanner {
        range: -1,
        regex: None,
    });
    let kvs = server.scan(scanner).unwrap().1;
    assert_eq!(kvs.len(), 0);
}

#[test]
fn merge() {
    const N: u8 = 5;

    let mut server = new_server(Options {
        file_size_limit: 200,
        keep_old_files: true,
        merge_policy: MergePolicy::Test,
    });

    for i in 0..N { server.put(vec![i; 8], vec![i; 256]).unwrap(); }
    for i in 0..N { server.put(vec![i; 8], vec![i; 256]).unwrap(); }

    server.merge().unwrap();

    for i in 0..N {
//        assert!(server.get(&vec![i; 8]).is_ok());
        assert_eq!(server.get(&vec![i; 8]).unwrap().unwrap(), vec![i; 256]);
    }

    assert_eq!(
        read_dir(STORAGE_DIR)
            .unwrap()
            .into_iter()
            .map(|e| e.unwrap())
            .filter(|e| if let Some(extension) = e.path().extension() { if extension == "data" || extension == "hint" { true } else { false } } else { false })
            .count(),
        N as usize * 2
    );
}

#[test]
fn data_file_scale_up() {
    const N: usize = 200;
    const TEST_DIR: &'static str = "tests/data/test-data-file-scale-up";

    fn count_files() -> usize {
        let mut count = 0;

        for entry in read_dir(TEST_DIR).unwrap() {
            let entry = entry.unwrap();
            if let Some(extension) = entry.path().extension() {
                if extension == "data" || extension == "hint" { count += 1; }
            }
        }

        count
    }

    let _ = remove_dir_all(TEST_DIR);

    let mut result_1 = HashMap::new();
    {
        let mut server = Server::new(HashEngineBuilder::new()
            .storage_dir(TEST_DIR)
            .options(Options {
                file_size_limit: 2,
                keep_old_files: false,
                merge_policy: MergePolicy::Test,
            })
            .build()
            .unwrap());

        for i in 0..N { server.put(vec![i as _], vec![i as _]).unwrap(); }
        assert_eq!(count_files(), N * 2);

        for i in 0..N {
            let k = vec![i as _];
            let v = server.get(&k).unwrap();
            assert!(v.is_some());
            let v = v.unwrap();
            result_1.insert(k, v);
        }
    }

    let mut result_2 = HashMap::new();
    {
        let mut server = Server::new(HashEngineBuilder::new()
            .storage_dir(TEST_DIR)
            .options(Options {
                file_size_limit: 5 * 0x100000,
                keep_old_files: false,
                merge_policy: MergePolicy::Test,
            })
            .build()
            .unwrap());

        server.merge().unwrap();
        assert_eq!(count_files(), 2);

        for i in 0..N {
            let k = vec![i as _];
            let v = server.get(&k).unwrap();
            assert!(v.is_some());
            let v = v.unwrap();
            result_2.insert(k, v);
        }
    }

    assert_eq!(result_1, result_2);
}

#[test]
fn data_file_scale_down() {
    const N: usize = 200;
    const TEST_DIR: &'static str = "tests/data/test-data-file-scale-down";

    fn count_files() -> usize {
        let mut count = 0;

        for entry in read_dir(TEST_DIR).unwrap() {
            let entry = entry.unwrap();
            if let Some(extension) = entry.path().extension() {
                if extension == "data" || extension == "hint" { count += 1; }
            }
        }

        count
    }

    let _ = remove_dir_all(TEST_DIR);

    let mut result_1 = HashMap::new();
    {
        let mut server = Server::new(HashEngineBuilder::new()
            .storage_dir(TEST_DIR)
            .options(Options {
                file_size_limit: 5 * 0x100000,
                keep_old_files: false,
                merge_policy: MergePolicy::Test,
            })
            .build()
            .unwrap());

        for i in 0..N { server.put(vec![i as _], vec![i as _]).unwrap(); }
        assert_eq!(count_files(), 2);

        for i in 0..N {
            let k = vec![i as _];
            let v = server.get(&k).unwrap();
            assert!(v.is_some());
            let v = v.unwrap();
            result_1.insert(k, v);
        }
    }

    let mut result_2 = HashMap::new();
    {
        let mut server = Server::new(HashEngineBuilder::new()
            .storage_dir(TEST_DIR)
            .options(Options {
                file_size_limit: 2,
                keep_old_files: false,
                merge_policy: MergePolicy::Test,
            })
            .build()
            .unwrap());

        server.merge().unwrap();
        assert_eq!(count_files(), N * 2);

        for i in 0..N {
            let k = vec![i as _];
            let v = server.get(&k).unwrap();
            assert!(v.is_some());
            let v = v.unwrap();
            result_2.insert(k, v);
        }
    }

    assert_eq!(result_1, result_2);
}

fn new_client() -> KvServerClient {
    // --- external ---
    use grpcio::{ChannelBuilder, EnvBuilder};

    KvServerClient::new(ChannelBuilder::new(Arc::new(EnvBuilder::new().build())).connect("127.0.0.1:23333"))
}

#[test]
fn client() {
    let client = new_client();
    loop {
        let mut request = Request::new();
        let input = {
            let mut s = String::new();
            io::stdin().read_line(&mut s).unwrap();

            s.trim().to_owned()
        };
        let s = input
            .split(' ')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        match s[0] {
            "put" => {
                request.set_operation(Operation::PUT);
                request.set_key(s[1].to_owned());
                request.set_value(s[2].to_owned());
            }
            "get" => {
                request.set_operation(Operation::GET);
                request.set_key(s[1].to_owned());
            }
            "del" => {
                request.set_operation(Operation::DEL);
                request.set_key(s[1].to_owned());
            }
            "scan" => {
                let mut scan_request = ScanRequest::new();
                scan_request.set_range(s[1].parse().unwrap());
                if s.len() > 2 { scan_request.set_regex(s[2].to_owned()); }

                let mut buffer = vec![client.scan(&scan_request).unwrap()];
                loop {
                    let f = buffer.pop().unwrap().into_future();
                    match f.wait() {
                        Ok((Some(scan_response), next)) => {
                            buffer.push(next);
                            println!("{:?}, K: {}, V: {}", scan_response.status, scan_response.key, scan_response.value);
                        }
                        Ok((None, _)) => break,
                        Err(_) => ()
                    }
                }

                continue;
            }
            "merge" => {
                request.set_operation(Operation::MERGE);
            }
            "exit" => break,
            cmd => {
                println!("Invalid command {}", cmd);
                continue;
            }
        }

        let response = client.serve(&request).unwrap();
        println!("{:?} {:?}, {}", request.operation, response.status, response.value);
    }
}

#[test]
fn put_with_client() {
    const N: usize = 200;

    let client = new_client();

    for i in 0..N {
        let mut request = Request::new();
        request.set_operation(Operation::PUT);
        request.set_key(i.to_string());
        request.set_value(i.to_string());

        let response = client.serve(&request).unwrap();
        println!("{:?} {:?}", request.operation, response.status);
    }
}

#[test]
fn get_with_client() {
    const N: usize = 200;

    let client = new_client();

    for i in 0..N {
        let mut request = Request::new();
        request.set_operation(Operation::GET);
        request.set_key(i.to_string());

        let response = client.serve(&request).unwrap();
        println!("{:?} {:?}, {}", request.operation, response.status, response.value);
//        assert_eq!(response.value, );
    }
}

#[test]
fn del_with_client() {
    const N: usize = 200;

    let client = new_client();

    for i in 0..N {
        let mut request = Request::new();
        request.set_operation(Operation::DEL);
        request.set_key(i.to_string());

        let response = client.serve(&request).unwrap();
        println!("{:?} {:?}", request.operation, response.status);
    }

    for i in 0..N {
        let mut request = Request::new();
        request.set_operation(Operation::GET);
        request.set_key(i.to_string());

        let response = client.serve(&request).unwrap();
        println!("{:?} {:?}", request.operation, response.status);
        assert_eq!(response.status, Status::NotFound);
    }
}
