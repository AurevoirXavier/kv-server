// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_KV_SERVER_SERVE: ::grpcio::Method<super::kv_server::Request, super::kv_server::Response> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kv_server.KVServer/Serve",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_SERVER_SCAN: ::grpcio::Method<super::kv_server::ScanRequest, super::kv_server::ScanResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/kv_server.KVServer/Scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct KvServerClient {
    client: ::grpcio::Client,
}

impl KvServerClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        KvServerClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn serve_opt(&self, req: &super::kv_server::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kv_server::Response> {
        self.client.unary_call(&METHOD_KV_SERVER_SERVE, req, opt)
    }

    pub fn serve(&self, req: &super::kv_server::Request) -> ::grpcio::Result<super::kv_server::Response> {
        self.serve_opt(req, ::grpcio::CallOption::default())
    }

    pub fn serve_async_opt(&self, req: &super::kv_server::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::Response>> {
        self.client.unary_call_async(&METHOD_KV_SERVER_SERVE, req, opt)
    }

    pub fn serve_async(&self, req: &super::kv_server::Request) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::Response>> {
        self.serve_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::kv_server::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::kv_server::ScanResponse>> {
        self.client.server_streaming(&METHOD_KV_SERVER_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::kv_server::ScanRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::kv_server::ScanResponse>> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item=(), Error=()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait KvServer {
    fn serve(&mut self, ctx: ::grpcio::RpcContext, req: super::kv_server::Request, sink: ::grpcio::UnarySink<super::kv_server::Response>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::kv_server::ScanRequest, sink: ::grpcio::ServerStreamingSink<super::kv_server::ScanResponse>);
}

pub fn create_kv_server<S: KvServer + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_SERVER_SERVE, move |ctx, req, resp| {
        instance.serve(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_KV_SERVER_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
