mod engine;
mod scanner;

pub use engine::{
    Engine,
    hash::{
        self,
        HashEngine, HashEngineBuilder, HashScanner,
    },
};
pub use scanner::Scanner;

// --- external ---
use failure::Error;
use grpcio::{RpcContext, ServerStreamingSink, UnarySink};
// --- custom ---
use crate::protos::{
    kv_server::{Request, Response, ScanRequest, ScanResponse, Status},
    kv_server_grpc::KvServer,
};

#[derive(Clone)]
pub struct Server<E: Engine> {
    engine: E
}

impl<E> Server<E> where E: Engine {
    pub fn new(engine: E) -> Self { Self { engine } }

    #[allow(dead_code)]
    pub fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), Error> { self.engine.put(k, v) }
    #[allow(dead_code)]
    pub fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>, Error> { self.engine.get(k) }
    #[allow(dead_code)]
    pub fn del(&mut self, k: &[u8]) -> Result<(), Error> { self.engine.del(k) }
    #[allow(dead_code)]
    pub fn scan(&mut self, scanner: Scanner) -> Result<(Scanner, Vec<(Vec<u8>, Vec<u8>)>), Error> { self.engine.scan(scanner) }
    #[allow(dead_code)]
    pub fn merge(&mut self) -> Result<(), Error> { self.engine.merge() }
}

impl<E> KvServer for Server<E> where E: Engine {
    fn serve(&mut self, ctx: RpcContext, req: Request, sink: UnarySink<Response>) {
        // --- external ---
        use futures::Future;
        // --- custom ---
        use crate::protos::kv_server::Operation;

        let mut response = Response::new();
        match req.operation {
            Operation::PUT => match self.put(req.key.clone().into_bytes(), req.value.clone().into_bytes()) {
                Ok(_) => response.set_status(Status::OK),
                Err(_) => response.set_status(Status::Err),
            }
            Operation::GET => match self.get(req.key.as_bytes()) {
                Ok(option) => if let Some(v) = option {
                    response.set_value(String::from_utf8_lossy(&v).to_string());
                    response.set_status(Status::OK);
                } else { response.set_status(Status::NotFound); }
                Err(_) => response.set_status(Status::Err),
            }
            Operation::DEL => match self.del(req.key.as_bytes()) {
                Ok(_) => response.set_status(Status::OK),
                Err(_) => response.set_status(Status::Err),
            }
            // TODO stream progress
            Operation::MERGE => match self.merge() {
                Ok(_) => response.set_status(Status::OK),
                Err(_) => response.set_status(Status::Err)
            }
        }

        let f = sink
            .success(response.clone())
            .map(move |_| println!("Responded with result"))
            .map_err(move |e| eprintln!("Failed to reply: {:?}", e));

        ctx.spawn(f);
    }

    fn scan(&mut self, ctx: RpcContext, req: ScanRequest, sink: ServerStreamingSink<ScanResponse>) {
        // --- external ---
        use futures::{Future, Sink, stream};
        use grpcio::{Error, WriteFlags};
        use regex::bytes::Regex;

        let scanner = Scanner::HashScanner(HashScanner {
            range: req.range,
            regex: if req.regex.is_empty() { None } else if let Ok(regex) = Regex::new(&req.regex) { Some(regex) } else {
                let mut scan_response = ScanResponse::new();
                scan_response.set_status(Status::InvalidRegex);
                let f = sink
                    .send_all(stream::iter_ok::<_, Error>(vec![(scan_response, WriteFlags::default())]))
                    .map(|_| println!("Responded with result"))
                    .map_err(move |e| eprintln!("Failed to handle scan request: {:?}", e));

                ctx.spawn(f);

                return;
            },
        });

        let mut data = vec![];
        let kvs = self.scan(scanner).unwrap().1;
        for (k, v) in kvs {
            let mut scan_response = ScanResponse::new();
            scan_response.set_status(Status::OK);
            scan_response.set_key(String::from_utf8_lossy(&k).to_string());
            scan_response.set_value(String::from_utf8_lossy(&v).to_string());
            data.push((scan_response, WriteFlags::default()));
        }

        let f = sink
            .send_all(stream::iter_ok::<_, Error>(data))
            .map(|_| println!("Responded with result"))
            .map_err(move |e| eprintln!("Failed to handle scan request: {:?}", e));

        ctx.spawn(f)
    }
}
