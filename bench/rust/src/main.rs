//! The dista taxi gate again, in Rust: taxibench's eight queries over the NYC
//! taxi Iceberg table, one task per data file, checked against the same scans
//! in one process. The Mojo version is `deploy/taxi.mojo`; the Ray version is
//! `bench/ray/taxi_ray.py`.
//!
//! One binary, two roles, like dista:
//!
//! * `TAXI_ROLE=worker` listens on `TAXI_PORT` (default 7400) and serves one
//!   task at a time: read the `FileScanTask` it was sent with iceberg-rust,
//!   fold it, and send back the partial (or the batches as Arrow IPC). A
//!   worker never opens the table or plans; it holds only a FileIO.
//! * Otherwise it's the driver. `TAXI_WORKERS` is `host:port`; a name that
//!   resolves to several addresses (a headless Kubernetes Service) is one
//!   worker per address. With no `TAXI_WORKERS` it starts four local workers
//!   itself.
//!
//! The driver plans each query once and ships the planned tasks as JSON,
//! without their partition, partition spec and name mapping (iceberg-rust
//! can't serialize those). That's safe for this table: it's partitioned by
//! month, not by identity, and its files carry field ids.
//!
//! The driver keeps one task in flight per worker and hands the next file to
//! whichever worker finishes first. There's no head: workers are addressed
//! directly, which is less machinery than dista or Ray carry.
//!
//! `TAXI_METADATA` is the table's metadata location, `file://` or `s3://`.
//! A local warehouse must be at the path the table was written at:
//! iceberg-rust reads manifests by their absolute locations, as PyIceberg
//! does. S3 is configured from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//! `AWS_SESSION_TOKEN`, `AWS_REGION` (or `AWS_DEFAULT_REGION`) and
//! `AWS_ENDPOINT_URL` (a custom endpoint, path style, for MinIO).
//! `TAXI_THREADS=1` runs each process on a single-threaded runtime, with
//! iceberg-rust's concurrency limits at `TAXI_FILES_AT_ONCE` (default 8):
//! one thread, with that many files' and manifests' reads in flight, which
//! is the best one thread does on object storage. `TAXI_PASSES` (default 1) runs each
//! query's distributed part that many times, printing `TIME <query> <pass>
//! <ms>` after each; pass 1 is checked against one process, later passes
//! against pass 1.
//!
//! `TAXI_MODE` picks what runs: `both` (the default) as above; `distributed`
//! skips the one-process scan, so pass 1 goes unchecked here and later
//! passes are checked against it; `local` runs only the one-process scan and
//! needs no workers. Each query's first answer is printed as `ANSWER <query>
//! <rows> <sum_a> <sum_b> <zones_total>`, so runs can be checked against each
//! other afterwards.

mod filter;

use std::collections::VecDeque;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::{cast, sum};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::scan::{FileScanTask, TableScan};
use iceberg::table::{StaticTable, Table};
use iceberg::{Runtime, TableIdent};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const ZONES: usize = 266;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Count,
    Sum,
    Ratio,
    ZoneCount,
    ZoneSum,
}

#[derive(Clone, Copy)]
struct Query {
    name: &'static str,
    filter: &'static str,
    columns: &'static [&'static str],
    kind: Kind,
    sum_column: &'static str,
}

/// The same eight as deploy/taxi.mojo, in the same filter DSL.
const QUERIES: [Query; 8] = [
    Query {
        name: "q1_scan_count",
        filter: r#"[">","trip_distance",0.0]"#,
        columns: &["trip_distance"],
        kind: Kind::Count,
        sum_column: "",
    },
    Query {
        name: "q2_month_range",
        filter: r#"["and",[">=","tpep_pickup_datetime","2024-03-01T00:00:00"],["<","tpep_pickup_datetime","2024-06-01T00:00:00"]]"#,
        columns: &["total_amount"],
        kind: Kind::Sum,
        sum_column: "total_amount",
    },
    Query {
        name: "q3_payment_sum",
        filter: r#"["=","payment_type",2]"#,
        columns: &["payment_type", "total_amount"],
        kind: Kind::Sum,
        sum_column: "total_amount",
    },
    Query {
        name: "q4_tip_ratio",
        filter: r#"["and",[">","trip_distance",10.0],[">","fare_amount",0.0]]"#,
        columns: &["trip_distance", "fare_amount", "tip_amount"],
        kind: Kind::Ratio,
        sum_column: "",
    },
    Query {
        name: "q5_top_zones",
        filter: r#"[">","trip_distance",0.0]"#,
        columns: &["PULocationID", "trip_distance"],
        kind: Kind::ZoneCount,
        sum_column: "",
    },
    Query {
        name: "q6_zone_revenue",
        filter: r#"["and",[">=","tpep_pickup_datetime","2024-01-01T00:00:00"],["<","tpep_pickup_datetime","2025-01-01T00:00:00"]]"#,
        columns: &["PULocationID", "total_amount"],
        kind: Kind::ZoneSum,
        sum_column: "",
    },
    Query {
        name: "q7_wide",
        filter: r#"["and",[">=","tpep_pickup_datetime","2024-06-01T00:00:00"],["<","tpep_pickup_datetime","2024-07-01T00:00:00"]]"#,
        columns: &[],
        kind: Kind::Count,
        sum_column: "",
    },
    Query {
        name: "q8_selective",
        filter: r#"["and",["=","PULocationID",132],["=","payment_type",1],[">","trip_distance",20.0]]"#,
        columns: &[
            "PULocationID",
            "payment_type",
            "trip_distance",
            "total_amount",
        ],
        kind: Kind::Sum,
        sum_column: "total_amount",
    },
];

/// What one file contributes; partials add up to the answer.
#[derive(Serialize, Deserialize, Clone)]
struct Partial {
    rows: u64,
    sum_a: f64,
    sum_b: f64,
    zones: Vec<f64>,
    node: String,
}

impl Partial {
    fn zero() -> Self {
        Partial {
            rows: 0,
            sum_a: 0.0,
            sum_b: 0.0,
            zones: vec![0.0; ZONES],
            node: String::new(),
        }
    }

    fn add(&mut self, o: &Partial) {
        self.rows += o.rows;
        self.sum_a += o.sum_a;
        self.sum_b += o.sum_b;
        for (a, b) in self.zones.iter_mut().zip(&o.zones) {
            *a += b;
        }
    }
}

fn column<'a>(b: &'a RecordBatch, name: &str) -> R<&'a Arc<dyn Array>> {
    b.column_by_name(name)
        .ok_or_else(|| format!("no column {name}").into())
}

fn f64_sum(b: &RecordBatch, name: &str) -> R<f64> {
    let c = cast(column(b, name)?, &DataType::Float64)?;
    Ok(sum(c.as_primitive::<Float64Type>()).unwrap_or(0.0))
}

fn fold(p: &mut Partial, q: &Query, b: &RecordBatch) -> R<()> {
    p.rows += b.num_rows() as u64;
    match q.kind {
        Kind::Count => {}
        Kind::Sum => p.sum_a += f64_sum(b, q.sum_column)?,
        Kind::Ratio => {
            p.sum_a += f64_sum(b, "tip_amount")?;
            p.sum_b += f64_sum(b, "fare_amount")?;
        }
        Kind::ZoneCount | Kind::ZoneSum => {
            let zone = cast(column(b, "PULocationID")?, &DataType::Int64)?;
            let zone = zone.as_primitive::<Int64Type>();
            let amount = match q.kind {
                Kind::ZoneSum => Some(cast(column(b, "total_amount")?, &DataType::Float64)?),
                _ => None,
            };
            let amount = amount.as_ref().map(|a| a.as_primitive::<Float64Type>());
            for i in 0..zone.len() {
                if zone.is_null(i) {
                    continue;
                }
                let z = zone.value(i);
                if !(0..ZONES as i64).contains(&z) {
                    continue;
                }
                match amount {
                    None => p.zones[z as usize] += 1.0,
                    Some(a) if !a.is_null(i) => p.zones[z as usize] += a.value(i),
                    Some(_) => {}
                }
            }
        }
    }
    Ok(())
}

fn single_threaded() -> bool {
    env::var("TAXI_THREADS").map(|v| v == "1").unwrap_or(false)
}

/// Files (and manifests) read at once on a single-threaded runtime.
fn files_at_once() -> usize {
    env::var("TAXI_FILES_AT_ONCE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(8)
}

/// A FileIO for `file://` and `s3://`, with S3 configured from the `AWS_*`
/// environment.
fn file_io() -> FileIO {
    let mut props = Vec::new();
    for (key, var) in [
        ("s3.access-key-id", "AWS_ACCESS_KEY_ID"),
        ("s3.secret-access-key", "AWS_SECRET_ACCESS_KEY"),
        ("s3.session-token", "AWS_SESSION_TOKEN"),
    ] {
        if let Ok(v) = env::var(var) {
            props.push((key, v));
        }
    }
    if let Ok(v) = env::var("AWS_REGION").or_else(|_| env::var("AWS_DEFAULT_REGION")) {
        props.push(("s3.region", v));
    }
    if let Ok(v) = env::var("AWS_ENDPOINT_URL") {
        props.push(("s3.endpoint", v));
        props.push(("s3.path-style-access", "true".into()));
    }
    FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
        .with_props(props)
        .build()
}

async fn open_table() -> R<Table> {
    let file_io = file_io();
    let metadata = env::var("TAXI_METADATA")?;
    let ident = TableIdent::from_strs(["taxi", "trips"])?;
    Ok(StaticTable::from_metadata_file(&metadata, ident, file_io)
        .await?
        .into_table())
}

fn build_scan(table: &Table, q: &Query) -> R<TableScan> {
    let predicate = filter::parse_filter(q.filter, table.metadata().current_schema())?;
    let mut b = table.scan().with_filter(predicate);
    b = if q.columns.is_empty() {
        b.select_all()
    } else {
        b.select(q.columns.iter().copied())
    };
    if single_threaded() {
        b = b.with_concurrency_limit(files_at_once());
    }
    Ok(b.build()?)
}

async fn plan(scan: &TableScan) -> R<Vec<FileScanTask>> {
    Ok(scan.plan_files().await?.try_collect().await?)
}

/// A planned task as it's sent to a worker: without the fields iceberg-rust
/// refuses to serialize.
fn shippable(mut t: FileScanTask) -> FileScanTask {
    t.partition = None;
    t.partition_spec = None;
    t.name_mapping = None;
    t
}

fn reader(file_io: &FileIO) -> ArrowReaderBuilder {
    let mut reader = ArrowReaderBuilder::new(file_io.clone(), Runtime::current());
    if single_threaded() {
        reader = reader.with_data_file_concurrency_limit(files_at_once());
    }
    reader
}

async fn read(file_io: &FileIO, tasks: Vec<FileScanTask>) -> R<Vec<RecordBatch>> {
    let stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    Ok(reader(file_io).build().read(stream)?.stream().try_collect().await?)
}

// ── worker ─────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct Request {
    query: usize,
    /// One data file, as the driver planned it.
    task: FileScanTask,
    batches: bool,
}

async fn send(s: &mut TcpStream, bytes: &[u8]) -> R<()> {
    s.write_all(&(bytes.len() as u64).to_le_bytes()).await?;
    s.write_all(bytes).await?;
    Ok(())
}

async fn recv(s: &mut BufReader<TcpStream>) -> R<Option<Vec<u8>>> {
    let mut len = [0u8; 8];
    if s.read_exact(&mut len).await.is_err() {
        return Ok(None);
    }
    let mut buf = vec![0u8; u64::from_le_bytes(len) as usize];
    s.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

async fn serve_one(file_io: &FileIO, req: Request) -> R<Vec<u8>> {
    let q = &QUERIES[req.query];
    let batches = read(file_io, vec![req.task]).await?;
    if req.batches {
        let mut out = Vec::new();
        if let Some(first) = batches.first() {
            let mut w = StreamWriter::try_new(&mut out, &first.schema())?;
            for b in &batches {
                w.write(b)?;
            }
            w.finish()?;
        }
        return Ok(out);
    }
    let mut p = Partial::zero();
    for b in &batches {
        fold(&mut p, q, b)?;
    }
    p.node = env::var("NODE_NAME").unwrap_or_else(|_| hostname());
    Ok(serde_json::to_vec(&p)?)
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| "localhost".into())
}

async fn worker() -> R<()> {
    let port = env::var("TAXI_PORT").unwrap_or_else(|_| "7400".into());
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    let file_io = file_io();
    loop {
        let (sock, _) = listener.accept().await?;
        sock.set_nodelay(true)?;
        let file_io = file_io.clone();
        tokio::spawn(async move {
            let mut sock = BufReader::new(sock);
            while let Ok(Some(msg)) = recv(&mut sock).await {
                let reply = match serde_json::from_slice::<Request>(&msg) {
                    Ok(req) => serve_one(&file_io, req).await.map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                };
                let bytes = match reply {
                    Ok(b) => [vec![0u8], b].concat(),
                    Err(e) => [vec![1u8], e.into_bytes()].concat(),
                };
                if send(sock.get_mut(), &bytes).await.is_err() {
                    break;
                }
            }
        });
    }
}

// ── driver ─────────────────────────────────────────────────────────────────

async fn connect_all() -> R<Vec<BufReader<TcpStream>>> {
    // A headless Service's name resolves to one address per ready pod, so
    // keep looking until `TAXI_EXPECT_WORKERS` of them are there.
    let spec = env::var("TAXI_WORKERS")?;
    let expect: usize = env::var("TAXI_EXPECT_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let mut addrs = Vec::new();
    for _ in 0..240 {
        addrs.clear();
        for part in spec.split(',') {
            if let Ok(found) = tokio::net::lookup_host(part).await {
                addrs.extend(found.filter(|a| a.is_ipv4()));
            }
        }
        addrs.sort();
        addrs.dedup();
        if addrs.len() >= expect {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let mut conns = Vec::new();
    for a in addrs {
        for attempt in 0..240 {
            match TcpStream::connect(a).await {
                Ok(s) => {
                    s.set_nodelay(true)?;
                    conns.push(BufReader::new(s));
                    break;
                }
                Err(e) if attempt == 239 => return Err(e.into()),
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(250)).await,
            }
        }
    }
    if conns.is_empty() {
        return Err("no workers".into());
    }
    Ok(conns)
}

/// Runs `tasks` across the workers, one in flight per worker; returns the
/// replies in completion order.
async fn run_tasks(
    conns: &mut [BufReader<TcpStream>],
    query: usize,
    tasks: Vec<FileScanTask>,
    batches: bool,
) -> R<Vec<Vec<u8>>> {
    let queue = Arc::new(Mutex::new(VecDeque::from(tasks)));
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut jobs = Vec::new();
    for conn in conns.iter_mut() {
        let queue = queue.clone();
        let results = results.clone();
        jobs.push(async move {
            loop {
                let Some(task) = queue.lock().unwrap().pop_front() else {
                    return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                };
                let req = serde_json::to_vec(&Request {
                    query,
                    task,
                    batches,
                })?;
                send(conn.get_mut(), &req).await?;
                let reply = recv(conn).await?.ok_or("worker went away")?;
                if reply[0] != 0 {
                    return Err(String::from_utf8_lossy(&reply[1..]).to_string().into());
                }
                results.lock().unwrap().push(reply[1..].to_vec());
            }
        });
    }
    for r in futures::future::join_all(jobs).await {
        r?;
    }
    let out = std::mem::take(&mut *results.lock().unwrap());
    Ok(out)
}

// Relative 1e-6: summing ~1.4e9 doubles in different orders differs by
// about 1e-9 relative (on AWS, q3 came to 7104909931.26 one way and
// 7104909941.44 the other).
fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-6 * a.abs().max(b.abs()).max(1.0)
}

fn answer(name: &str, p: &Partial) {
    let zones: f64 = p.zones.iter().sum();
    println!("ANSWER {} {} {:?} {:?} {:?}", name, p.rows, p.sum_a, p.sum_b, zones);
}

fn mode() -> R<String> {
    let m = env::var("TAXI_MODE").unwrap_or_else(|_| "both".into());
    match m.as_str() {
        "both" | "distributed" | "local" => Ok(m),
        _ => Err(format!("TAXI_MODE must be both, distributed or local, got {m}").into()),
    }
}

fn check(name: &str, got: &Partial, want: &Partial) -> R<()> {
    if got.rows != want.rows {
        return Err(format!("{name}: {} rows distributed, {} local", got.rows, want.rows).into());
    }
    if !close(got.sum_a, want.sum_a) || !close(got.sum_b, want.sum_b) {
        return Err(format!(
            "{name}: sums {}/{} vs {}/{}",
            got.sum_a, got.sum_b, want.sum_a, want.sum_b
        )
        .into());
    }
    for i in 0..ZONES {
        if !close(got.zones[i], want.zones[i]) {
            return Err(format!("{name}: zone {i} {} vs {}", got.zones[i], want.zones[i]).into());
        }
    }
    Ok(())
}

async fn local_fold(table: &Table, q: &Query) -> R<Partial> {
    let scan = build_scan(table, q)?;
    let tasks = plan(&scan).await?;
    if single_threaded() {
        return fold_tasks(table.file_io().clone(), *q, tasks).await;
    }
    // iceberg-rust's reader yields one stream, and whoever polls it decodes
    // every file: polled from one task, a multi-threaded runtime still
    // decodes on one core. So the files are dealt out to one spawned task
    // per core, each folding its share, and the partials added up.
    let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let mut shares: Vec<Vec<FileScanTask>> = (0..n).map(|_| Vec::new()).collect();
    for (i, t) in tasks.into_iter().enumerate() {
        shares[i % n].push(t);
    }
    let mut handles = Vec::new();
    for share in shares.into_iter().filter(|s| !s.is_empty()) {
        let io = table.file_io().clone();
        let q = *q;
        handles.push(tokio::spawn(async move {
            fold_tasks(io, q, share).await.map_err(|e| e.to_string())
        }));
    }
    let mut p = Partial::zero();
    for h in handles {
        p.add(&h.await?.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?);
    }
    Ok(p)
}

/// One stream of `tasks`, folded as it's read.
async fn fold_tasks(io: FileIO, q: Query, tasks: Vec<FileScanTask>) -> R<Partial> {
    let stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    let mut batches = reader(&io).build().read(stream)?.stream();
    let mut p = Partial::zero();
    while let Some(b) = batches.next().await {
        fold(&mut p, &q, &b?)?;
    }
    Ok(p)
}

fn spawn_local_workers() -> R<Vec<std::process::Child>> {
    let me = env::current_exe()?;
    let mut kids = Vec::new();
    let mut addrs = Vec::new();
    for i in 0..4 {
        let port = 7401 + i;
        kids.push(
            std::process::Command::new(&me)
                .env("TAXI_ROLE", "worker")
                .env("TAXI_PORT", port.to_string())
                .spawn()?,
        );
        addrs.push(format!("127.0.0.1:{port}"));
    }
    env::set_var("TAXI_WORKERS", addrs.join(","));
    Ok(kids)
}

async fn driver() -> R<()> {
    if mode()? == "local" {
        return drive_local().await;
    }
    let mut kids = Vec::new();
    if env::var("TAXI_WORKERS").is_err() {
        kids = spawn_local_workers()?;
    }
    let result = drive().await;
    for k in &mut kids {
        let _ = k.kill();
    }
    result
}

/// `TAXI_MODE=local`: the one-process scans alone, with no workers.
async fn drive_local() -> R<()> {
    let table = open_table().await?;
    let mut total_local = 0u128;
    for q in QUERIES.iter() {
        let t1 = Instant::now();
        let want = local_fold(&table, q).await?;
        let local = t1.elapsed().as_millis();
        total_local += local;
        answer(q.name, &want);
        println!(
            "{} : {} rows, sum {} | distributed -, one process {} ms",
            q.name, want.rows, want.sum_a, local
        );
    }
    println!("PASS rust taxi: 8 queries in one process; one process {total_local} ms");
    Ok(())
}

async fn drive() -> R<()> {
    let check_local = mode()? == "both";
    let mut conns = connect_all().await?;
    let table = open_table().await?;
    let passes: usize = env::var("TAXI_PASSES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1);
    let mut nodes: Vec<String> = Vec::new();
    let (mut total_dist, mut total_local) = (0u128, 0u128);
    for (qi, q) in QUERIES.iter().enumerate() {
        let mut first: Option<(Partial, usize, u128)> = None;
        for pass in 1..=passes {
            let t0 = Instant::now();
            let scan = build_scan(&table, q)?;
            let tasks: Vec<FileScanTask> = plan(&scan).await?.into_iter().map(shippable).collect();
            let n_files = tasks.len();
            let mut got = Partial::zero();
            for reply in run_tasks(&mut conns, qi, tasks, false).await? {
                let p: Partial = serde_json::from_slice(&reply)?;
                if !nodes.contains(&p.node) {
                    nodes.push(p.node.clone());
                }
                got.add(&p);
            }
            let dist = t0.elapsed().as_millis();
            println!("TIME {} {} {}", q.name, pass, dist);
            match &first {
                Some((want, _, _)) => check(q.name, &got, want)?,
                None => first = Some((got, n_files, dist)),
            }
        }
        let (got, n_files, dist) = first.unwrap();
        total_dist += dist;
        let local = if check_local {
            let t1 = Instant::now();
            let want = local_fold(&table, q).await?;
            let local = t1.elapsed().as_millis();
            check(q.name, &got, &want)?;
            total_local += local;
            format!("{local} ms")
        } else {
            "-".to_string()
        };
        answer(q.name, &got);
        println!(
            "{} : {} files, {} rows, sum {} | distributed {} ms, one process {}",
            q.name, n_files, got.rows, got.sum_a, dist, local
        );
    }

    // Arrow across nodes: the quarter's total_amount comes back as IPC.
    let q = &QUERIES[1];
    let t2 = Instant::now();
    let scan = build_scan(&table, q)?;
    let tasks: Vec<FileScanTask> = plan(&scan).await?.into_iter().map(shippable).collect();
    let mut got = Partial::zero();
    let mut bytes = 0usize;
    for reply in run_tasks(&mut conns, 1, tasks, true).await? {
        bytes += reply.len();
        if reply.is_empty() {
            continue;
        }
        for b in StreamReader::try_new(std::io::Cursor::new(reply), None)? {
            fold(&mut got, q, &b?)?;
        }
    }
    // Without the one-process scan, the batches are checked against the
    // folded answer the distributed q2 gave.
    let want = if check_local {
        local_fold(&table, q).await?
    } else {
        let scan = build_scan(&table, q)?;
        let tasks: Vec<FileScanTask> = plan(&scan).await?.into_iter().map(shippable).collect();
        let mut p = Partial::zero();
        for reply in run_tasks(&mut conns, 1, tasks, false).await? {
            p.add(&serde_json::from_slice(&reply)?);
        }
        p
    };
    check("q2_as_batches", &got, &want)?;
    println!(
        "q2_as_batches : {} rows as Arrow, {} bytes over sockets, {} ms",
        got.rows,
        bytes,
        t2.elapsed().as_millis()
    );

    println!("tasks ran on {} nodes: {}", nodes.len(), nodes.join(" "));
    let expect: usize = env::var("DISTA_EXPECT_NODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if nodes.len() < expect {
        return Err(format!("expected {expect} nodes, saw {}", nodes.len()).into());
    }
    if check_local {
        println!(
            "PASS rust taxi: 8 queries + 1 as batches, distributed == one process; distributed {total_dist} ms, one process {total_local} ms"
        );
    } else {
        println!(
            "PASS rust taxi: 8 queries + 1 as batches, distributed passes agree; distributed {total_dist} ms"
        );
    }
    Ok(())
}

fn main() -> R<()> {
    let rt = if single_threaded() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
    };
    rt.block_on(async {
        if env::var("TAXI_ROLE").as_deref() == Ok("worker") {
            worker().await
        } else {
            driver().await
        }
    })
}
