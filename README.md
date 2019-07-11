## KV-Server

### Quick Start

Follow the steps below:

1. `git clone git@github.com:AurevoirXavier/kv-server.git`
2. `cd kv-server`
3. `cargo run --package kv-server --bin kv-server`
4. open a new console and go to the `kv-server` directory
5. `cargo test --package kv-server --test test client -- --exact`

Now you got a interactive client: 

Syntax:

```text
put [key: String] [value: String]
get [key: String]
del [key: String]
scan [range: isize (set -1 to scan the whole map)] [regex (optional): String]
merge
exit
```

```text
λ: put 1 1
> PUT OK, 

λ: get 1
> GET OK, 1

λ: del 1
> DEL OK,
 
λ: get 1
> GET NotFound,
 
λ: put 1 1
> PUT OK,
 
λ: put 2 2
> PUT OK,
 
λ: put 234 234
> PUT OK,
 
λ: scan 1
> OK, K: 2, V: 2

λ: scan 3
> OK, K: 2, V: 2
> OK, K: 234, V: 234
> OK, K: 1, V: 1

λ: scan 5
> OK, K: 2, V: 2
> OK, K: 234, V: 234
> OK, K: 1, V: 1

λ: scan 5 ^2.+4$
> OK, K: 234, V: 234

λ: exit
> bye~

test client ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out
``` 

More examples see the [tests](https://github.com/AurevoirXavier/kv-server/blob/master/tests/test.rs)

### C/S

Using gRPC with [grpc-rs](https://github.com/pingcap/grpc-rs)

```proto
service KVServer {
    rpc Serve (Request) returns (Response) {}
    rpc Scan (ScanRequest) returns (stream ScanResponse) {}
    
    enum Operation {
        PUT = 0;
        GET = 1;
        DEL = 2;
        MERGE = 3;
    }
    
    message Request {
        Operation operation = 1;
        ...
    }
    
    message Response { ... }
    
    message ScanRequest { ... }
    
    message ScanResponse { ... }
}
```

### Storage engine

- [ ] BTree engine
- [x] Hash engine
- [ ] LSMTree engine

```rust
pub trait Engine {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), Error>;
    fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn del(&mut self, k: &[u8]) -> Result<(), Error>;
    fn scan(&mut self, scanner: Scanner) -> Result<(Scanner, Vec<(Vec<u8>, Vec<u8>)>), Error>;
    fn merge(&mut self) -> Result<(), Error>;
}
```

### Hash Engine

Based on [bitcask](https://en.wikipedia.org/wiki/Bitcask) model

```rust
pub struct HashEngine {
    options: Options,

    storage_dir: String,
    key_dirs: Arc<RwLock<KeyDirs>>,

    active_file: DHFile,
    old_files: DataFiles,
}
```

#### K/V

- map: [hashbrown](https://github.com/rust-lang/hashbrown)
- key:
    - size up to `u32::max_value()`
- value:
    - size up to `u32::max_value()`

#### Features

- [x] data file scale
- [x] thread safe
- [x] persistence / recover

**API**

- [x] get
- [x] put
- [x] del
- [x] scan (with [regex](https://github.com/rust-lang/regex))
- [x] merge

### TODO

- Friendly log
- BTree engine
- LSMTree engine
- Hash engine
    - a redis-like scan cursor
    - merge policy
