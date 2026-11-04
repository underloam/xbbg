//! Test fixtures built at run time: wheels with RECORD, conda packages with paths.json, conda
//! channels, and a tiny HTTP/1.1 server for PEP 503/691 indexes.

#![allow(dead_code, unreachable_pub)]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use rattler_conda_types::compression_level::CompressionLevel;
use xbbg_sdk::{NetworkOptions, ProvisionOptions, ProxyConfig};

pub fn sha256(bytes: &[u8]) -> String {
    hex::encode(rattler_digest::compute_bytes_digest::<rattler_digest::Sha256>(bytes))
}

/// Options for a test cache: direct connections (never a system proxy), no retry delays.
pub fn options(cache: &Path) -> ProvisionOptions {
    let mut options = ProvisionOptions::new(cache);
    options.network.proxy = ProxyConfig::Direct;
    options.network.retries = 0;
    options
}

pub fn offline(mut options: ProvisionOptions) -> ProvisionOptions {
    options.network.offline = true;
    options
}

pub fn network() -> NetworkOptions {
    NetworkOptions::default()
}

// ---------------------------------------------------------------------------------------------
// Wheels

/// A `blpapi` wheel with a library, licences and a RECORD.
#[derive(Clone)]
pub struct Wheel {
    pub version: String,
    /// `<python>-<abi>-<platform>`, e.g. `py3-none-win_amd64`.
    pub tag: String,
    /// Member path of the library, e.g. `blpapi/blpapi3_64.dll`.
    pub lib_member: String,
    pub lib: Vec<u8>,
    /// Write a wrong RECORD hash for the library.
    pub corrupt_record: bool,
}

impl Wheel {
    pub fn windows(version: &str) -> Self {
        Self::new(version, "py3-none-win_amd64", "blpapi/blpapi3_64.dll")
    }

    pub fn linux(version: &str) -> Self {
        Self::new(
            version,
            "py3-none-manylinux2014_x86_64",
            "blpapi/libblpapi3_64.so",
        )
    }

    pub fn macos(version: &str) -> Self {
        Self::new(
            version,
            "py3-none-macosx_11_0_arm64",
            "blpapi/libblpapi3_64.so",
        )
    }

    pub fn new(version: &str, tag: &str, lib_member: &str) -> Self {
        Self {
            version: version.to_string(),
            tag: tag.to_string(),
            lib_member: lib_member.to_string(),
            lib: format!("fake {lib_member} {version} {tag}").into_bytes(),
            corrupt_record: false,
        }
    }

    pub fn filename(&self) -> String {
        format!("blpapi-{}-{}.whl", self.version, self.tag)
    }

    pub fn bytes(&self) -> Vec<u8> {
        let dist_info = format!("blpapi-{}.dist-info", self.version);
        let members: Vec<(String, Vec<u8>)> = vec![
            ("blpapi/__init__.py".into(), b"# blpapi\n".to_vec()),
            (self.lib_member.clone(), self.lib.clone()),
            (
                format!("{dist_info}/METADATA"),
                format!("Name: blpapi\nVersion: {}\n", self.version).into_bytes(),
            ),
            (
                format!("{dist_info}/WHEEL"),
                b"Wheel-Version: 1.0\n".to_vec(),
            ),
            (
                format!("{dist_info}/licenses/License.txt"),
                b"Bloomberg licence\n".to_vec(),
            ),
            (
                format!("{dist_info}/licenses/cpp-licenses/LICENSE.openssl"),
                b"OpenSSL licence\n".to_vec(),
            ),
        ];
        let mut record = String::new();
        for (name, data) in &members {
            let digest = rattler_digest::compute_bytes_digest::<rattler_digest::Sha256>(data);
            let mut hash = URL_SAFE_NO_PAD.encode(digest);
            if self.corrupt_record && *name == self.lib_member {
                hash = URL_SAFE_NO_PAD.encode([7u8; 32]);
            }
            record.push_str(&format!("{name},sha256={hash},{}\n", data.len()));
        }
        record.push_str(&format!("{dist_info}/RECORD,,\n"));

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (name, data) in members
            .iter()
            .chain([(format!("{dist_info}/RECORD"), record.into_bytes())].iter())
        {
            writer.start_file(name.as_str(), options).unwrap();
            writer.write_all(data).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    pub fn write_to(&self, dir: &Path) -> PathBuf {
        fs::create_dir_all(dir).unwrap();
        let path = dir.join(self.filename());
        fs::write(&path, self.bytes()).unwrap();
        path
    }
}

/// PEP 503 project page with Bloomberg-style relative links (`../../files/<name>#sha256=`).
pub fn simple_html(files: &[(String, Vec<u8>)], yanked: &[&str], wrong_hash: &[&str]) -> String {
    let mut html = String::from("<!DOCTYPE html>\n<html><body><h1>Links for blpapi</h1>\n");
    for (name, bytes) in files {
        let hash = if wrong_hash.contains(&name.as_str()) {
            "0".repeat(64)
        } else {
            sha256(bytes)
        };
        let yank = if yanked.contains(&name.as_str()) {
            " data-yanked=\"broken\""
        } else {
            ""
        };
        html.push_str(&format!(
            "<a href=\"../../files/{name}#sha256={hash}\" data-requires-python=\"&gt;=3.8\"{yank}>{name}</a><br/>\n"
        ));
    }
    html.push_str("</body></html>\n");
    html
}

/// PEP 691 project page with absolute URLs.
pub fn simple_json(base: &str, files: &[(String, Vec<u8>)]) -> String {
    let entries: Vec<serde_json::Value> = files
        .iter()
        .map(|(name, bytes)| {
            serde_json::json!({
                "filename": name,
                "url": format!("{base}/files/{name}"),
                "hashes": {"sha256": sha256(bytes)},
                "requires-python": ">=3.8",
                "yanked": false,
            })
        })
        .collect();
    serde_json::json!({"meta": {"api-version": "1.1"}, "name": "blpapi", "files": entries})
        .to_string()
}

// ---------------------------------------------------------------------------------------------
// conda packages and channels

#[derive(Clone)]
pub struct CondaPackage {
    pub name: String,
    pub version: String,
    pub build: String,
    pub build_number: u64,
    pub subdir: String,
    /// `.conda` or `.tar.bz2`.
    pub extension: String,
    /// Path of the library inside the package.
    pub lib_path: String,
    pub lib: Vec<u8>,
    /// Write a wrong sha256 for the library into info/paths.json.
    pub corrupt_paths_json: bool,
}

impl CondaPackage {
    pub fn windows(version: &str, build_number: u64, extension: &str) -> Self {
        Self::new(
            version,
            build_number,
            "win-64",
            extension,
            "Library/bin/blpapi3_64.dll",
        )
    }

    pub fn linux(version: &str, build_number: u64, extension: &str) -> Self {
        Self::new(
            version,
            build_number,
            "linux-64",
            extension,
            "lib/libblpapi3_64.so",
        )
    }

    pub fn new(
        version: &str,
        build_number: u64,
        subdir: &str,
        extension: &str,
        lib_path: &str,
    ) -> Self {
        Self {
            name: "libblpapi".into(),
            version: version.into(),
            build: format!("h0123abc_{build_number}"),
            build_number,
            subdir: subdir.into(),
            extension: extension.into(),
            lib_path: lib_path.into(),
            lib: format!("fake conda {lib_path} {version} {build_number}").into_bytes(),
            corrupt_paths_json: false,
        }
    }

    pub fn filename(&self) -> String {
        format!(
            "{}-{}-{}{}",
            self.name, self.version, self.build, self.extension
        )
    }

    /// Writes the package into `dir` and returns its path.
    pub fn write_to(&self, dir: &Path) -> PathBuf {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        let mut files: Vec<(String, Vec<u8>)> = vec![(self.lib_path.clone(), self.lib.clone())];
        let other = self.lib_path.replace("3_64", "3_32");
        files.push((other, b"32-bit library".to_vec()));
        let mut paths = Vec::new();
        for (relative, data) in &files {
            let path = base.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, data).unwrap();
            let hash = if self.corrupt_paths_json && *relative == self.lib_path {
                "1".repeat(64)
            } else {
                sha256(data)
            };
            paths.push(serde_json::json!({
                "_path": relative,
                "path_type": "hardlink",
                "sha256": hash,
                "size_in_bytes": data.len(),
            }));
        }
        let info = base.join("info");
        fs::create_dir_all(info.join("licenses")).unwrap();
        fs::write(
            info.join("licenses").join("License.txt"),
            b"Bloomberg licence\n",
        )
        .unwrap();
        fs::write(
            info.join("paths.json"),
            serde_json::json!({"paths": paths, "paths_version": 1}).to_string(),
        )
        .unwrap();
        fs::write(
            info.join("index.json"),
            serde_json::json!({
                "name": self.name,
                "version": self.version,
                "build": self.build,
                "build_number": self.build_number,
                "subdir": self.subdir,
                "depends": [],
                "license": "LicenseRef-Bloomberg",
            })
            .to_string(),
        )
        .unwrap();
        // rattler's writer takes absolute paths under `base` and stores them relative to it.
        let mut members: Vec<PathBuf> = files
            .iter()
            .map(|(relative, _)| base.join(relative))
            .collect();
        members.extend(
            [
                "info/index.json",
                "info/paths.json",
                "info/licenses/License.txt",
            ]
            .iter()
            .map(|relative| base.join(relative)),
        );

        fs::create_dir_all(dir).unwrap();
        let out = dir.join(self.filename());
        let file = fs::File::create(&out).unwrap();
        if self.extension == ".conda" {
            let stem = self.filename().trim_end_matches(".conda").to_string();
            rattler_package_streaming::write::write_conda_package(
                file,
                base,
                &members,
                CompressionLevel::Default,
                Some(1),
                &stem,
                None,
                None,
            )
            .unwrap();
        } else {
            rattler_package_streaming::write::write_tar_bz2_package(
                file,
                base,
                &members,
                CompressionLevel::Default,
                None,
                None,
            )
            .unwrap();
        }
        out
    }

    /// The repodata entry for the package at `path`.
    pub fn record(&self, path: &Path, sha_override: Option<&str>) -> serde_json::Value {
        let bytes = fs::read(path).unwrap();
        serde_json::json!({
            "name": self.name,
            "version": self.version,
            "build": self.build,
            "build_number": self.build_number,
            "depends": [],
            "subdir": self.subdir,
            "sha256": sha_override.map_or_else(|| sha256(&bytes), str::to_string),
            "size": bytes.len(),
            "timestamp": 1_790_000_000_000u64 + self.build_number,
        })
    }
}

/// `packages` (`.tar.bz2`) and `packages.conda` entries of one subdir's repodata.
type Entries = serde_json::Map<String, serde_json::Value>;

/// Writes a channel `<root>/<subdir>/{repodata.json, packages}`; returns the channel root.
pub fn conda_channel(root: &Path, packages: &[(CondaPackage, Option<&str>)]) -> PathBuf {
    let mut by_subdir: HashMap<String, (Entries, Entries)> = HashMap::new();
    for (package, sha_override) in packages {
        let dir = root.join(&package.subdir);
        let path = package.write_to(&dir);
        let entry = by_subdir.entry(package.subdir.clone()).or_default();
        let record = package.record(&path, *sha_override);
        if package.extension == ".conda" {
            entry.1.insert(package.filename(), record);
        } else {
            entry.0.insert(package.filename(), record);
        }
    }
    for (subdir, (tar_bz2, conda)) in by_subdir {
        let repodata = serde_json::json!({
            "info": {"subdir": subdir},
            "packages": tar_bz2,
            "packages.conda": conda,
            "repodata_version": 1,
        });
        fs::write(
            root.join(&subdir).join("repodata.json"),
            repodata.to_string(),
        )
        .unwrap();
    }
    root.to_path_buf()
}

// ---------------------------------------------------------------------------------------------
// HTTP server

#[derive(Clone)]
pub struct Response {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

impl Response {
    pub fn ok(content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            content_type: content_type.into(),
            body: body.into(),
        }
    }
}

/// HTTP/1.1 server on 127.0.0.1 serving fixed routes; counts requests per path and can demand
/// basic credentials.
pub struct Server {
    pub base: String,
    hits: Arc<Mutex<HashMap<String, usize>>>,
    auth_headers: Arc<Mutex<Vec<Option<String>>>>,
}

impl Server {
    pub fn start(routes: HashMap<String, Response>) -> Self {
        Self::start_with_auth(routes, None)
    }

    /// `basic`: `user:password` every request must present.
    pub fn start_with_auth(routes: HashMap<String, Response>, basic: Option<&str>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(Mutex::new(HashMap::new()));
        let auth_headers = Arc::new(Mutex::new(Vec::new()));
        let expected = basic.map(|basic| format!("Basic {}", STANDARD.encode(basic)));
        let routes = Arc::new(routes);
        let (hits_in, auth_in) = (hits.clone(), auth_headers.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (routes, hits, auth, expected) = (
                    routes.clone(),
                    hits_in.clone(),
                    auth_in.clone(),
                    expected.clone(),
                );
                std::thread::spawn(move || {
                    serve(stream, &routes, &hits, &auth, expected.as_deref())
                });
            }
        });
        Self {
            base,
            hits,
            auth_headers,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    pub fn total_hits(&self) -> usize {
        self.hits.lock().unwrap().values().sum()
    }

    pub fn authorization_headers(&self) -> Vec<Option<String>> {
        self.auth_headers.lock().unwrap().clone()
    }
}

fn serve(
    stream: TcpStream,
    routes: &HashMap<String, Response>,
    hits: &Mutex<HashMap<String, usize>>,
    auth: &Mutex<Vec<Option<String>>>,
    expected: Option<&str>,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let path = target
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .to_string();
    let mut authorization = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("authorization")
        {
            authorization = Some(value.trim().to_string());
        }
    }
    *hits.lock().unwrap().entry(path.clone()).or_default() += 1;
    auth.lock().unwrap().push(authorization.clone());
    let response = if expected.is_some() && authorization.as_deref() != expected {
        Response {
            status: 401,
            content_type: "text/plain".into(),
            body: b"unauthorized".to_vec(),
        }
    } else {
        routes.get(&path).cloned().unwrap_or(Response {
            status: 404,
            content_type: "text/plain".into(),
            body: b"not found".to_vec(),
        })
    };
    let reason = match response.status {
        200 => "OK",
        401 => "Unauthorized",
        _ => "Not Found",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    if response.status == 401 {
        head.push_str("WWW-Authenticate: Basic realm=\"test\"\r\n");
    }
    head.push_str("\r\n");
    let mut stream = stream;
    let _ = stream.write_all(head.as_bytes());
    if method != "HEAD" {
        let _ = stream.write_all(&response.body);
    }
    let _ = stream.flush();
}
