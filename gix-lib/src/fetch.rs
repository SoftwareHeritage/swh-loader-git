// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Network / smart-protocol layer: fetching packs over HTTP(S).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result};
use bstr::BString;
use gix_hash::ObjectId;
use gix_protocol::handshake::Ref;
use gix_transport::client::blocking_io::connect::{connect, Options as ConnectOptions};
use gix_transport::client::blocking_io::{ExtendedBufRead, Transport};
use gix_transport::packetline::read::ProgressAction;
use gix_transport::{Protocol, Service};

/// Result of a successful `fetch_pack` call.
pub struct FetchPackResult {
    /// Maps ref name (e.g. `refs/heads/main`) → SHA-1 hex string (40 chars).
    pub remote_refs: HashMap<BString, String>,
    /// Maps symbolic ref name → target ref name (e.g. `HEAD` → `refs/heads/main`).
    pub symbolic_refs: HashMap<BString, BString>,
    /// Raw pack data bytes (empty if nothing new to fetch).
    pub pack_bytes: Vec<u8>,
}

/// Build the HTTP transport options carrying the caller's timeouts, if any.
///
/// `connect_timeout_secs` maps directly to curl's connect timeout.
/// `read_timeout_secs` maps to curl's low-speed abort
/// (`CURLOPT_LOW_SPEED_LIMIT`/`_TIME`): curl has no per-read timeout, so
/// "fewer than 1 byte/sec sustained for N seconds" is the idiomatic
/// equivalent of urllib3's read timeout for the stalled-server case that
/// setting exists to catch.  All other options keep their defaults
/// (notably `ssl_verify: true`).
fn http_timeout_options(
    connect_timeout_secs: Option<u64>,
    read_timeout_secs: Option<u64>,
) -> Option<gix_transport::client::blocking_io::http::Options> {
    if connect_timeout_secs.is_none() && read_timeout_secs.is_none() {
        return None;
    }
    let mut opts = gix_transport::client::blocking_io::http::Options::default();
    opts.connect_timeout = connect_timeout_secs.map(std::time::Duration::from_secs);
    if let Some(secs) = read_timeout_secs {
        opts.low_speed_limit_bytes_per_second = 1;
        opts.low_speed_time_seconds = secs;
    }
    Some(opts)
}

/// Apply [`http_timeout_options`] to a freshly-connected transport.
/// Must be called before the handshake; the curl backend reads the
/// options on each request it executes.
fn apply_timeouts(
    transport: &mut (dyn Transport + Send),
    connect_timeout_secs: Option<u64>,
    read_timeout_secs: Option<u64>,
) -> Result<()> {
    if let Some(opts) = http_timeout_options(connect_timeout_secs, read_timeout_secs) {
        transport
            .configure(&opts)
            .map_err(|e| anyhow::anyhow!("failed to configure HTTP transport timeouts: {e}"))?;
    }
    Ok(())
}

/// Fetch a pack from a remote git repository over HTTP/HTTPS.
///
/// * `url`        — remote URL (http:// or https://)
/// * `wants`      — list of 20-byte SHA-1 object IDs to request
/// * `haves`      — list of 20-byte SHA-1 object IDs we already have (for delta compression)
/// * `size_limit` — maximum pack size in bytes (0 = unlimited); returns error if exceeded
/// * `connect_timeout_secs` / `read_timeout_secs` — HTTP timeouts; see [`http_timeout_options`]
pub fn fetch_pack(
    url: &str,
    wants: Vec<[u8; 20]>,
    haves: Vec<[u8; 20]>,
    size_limit: u64,
    connect_timeout_secs: Option<u64>,
    read_timeout_secs: Option<u64>,
) -> Result<FetchPackResult> {
    // 1. Parse URL
    let url_parsed =
        gix_url::parse(url.into()).with_context(|| format!("invalid git URL: {url}"))?;

    // 2. Connect (blocking HTTP transport via curl).
    let options = ConnectOptions {
        version: Protocol::V1,
        ..Default::default()
    };
    let mut transport =
        connect(url_parsed, options).with_context(|| format!("failed to connect to {url}"))?;
    apply_timeouts(&mut *transport, connect_timeout_secs, read_timeout_secs)?;

    // 3. Handshake, parse refs, and release the transport borrow — all inside a block
    //    so that `outcome` (which borrows `transport`) is dropped before we reuse it
    //    for the actual fetch.
    let (actual_protocol, capabilities, parsed_refs) = {
        let mut outcome = transport
            .handshake(Service::UploadPack, &[])
            .context("git handshake failed")?;
        let actual_protocol = outcome.actual_protocol;

        // 4. Parse refs from the raw packet-line reader.
        let (parsed_refs, _shallow_updates) = match outcome.refs.take() {
            Some(mut reader) => {
                gix_protocol::handshake::refs::from_v1_refs_received_as_part_of_handshake_and_capabilities(
                    &mut *reader,
                    outcome.capabilities.iter(),
                )
                .context("failed to parse remote refs")?
            }
            None => (vec![], vec![]),
        };

        // Capabilities are valid to use now (refs reader consumed above).
        // `outcome` (and its borrow of `transport`) is dropped at end of block.
        let capabilities = outcome.capabilities;
        (actual_protocol, capabilities, parsed_refs)
    }; // outcome dropped here — transport borrow released

    // 5. Build remote_refs / symbolic_refs maps from parsed_refs.
    let mut remote_refs: HashMap<BString, String> = HashMap::new();
    let mut symbolic_refs: HashMap<BString, BString> = HashMap::new();

    for r in &parsed_refs {
        match r {
            Ref::Direct {
                full_ref_name,
                object,
            } => {
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Symbolic {
                full_ref_name,
                target,
                object,
                ..
            } => {
                symbolic_refs.insert(full_ref_name.clone(), target.clone());
                // Also record the resolved OID.
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Peeled {
                full_ref_name,
                tag,
                object,
            } => {
                // Bare ref name → the annotated-tag object's own OID.  This is
                // what becomes the SWH Release and the snapshot branch target;
                // it mirrors dulwich, which reports the un-peeled tag target.
                // gitoxide consolidates the `<name>` and `<name>^{}` advertised
                // lines into a single `Ref::Peeled` carrying both OIDs, so this
                // bare-name entry exists nowhere else.  Dropping `tag` here (the
                // former `..`) meant the tag OID reached neither `wants` nor
                // `remote_refs` and every annotated tag — and its Release — was
                // silently lost.
                remote_refs.insert(full_ref_name.clone(), tag.to_hex().to_string());
                // Peeled target — the commit the tag dereferences to, stored
                // under `<name>^{}` (standard git convention).  Filtered out
                // downstream, but kept for parity with dulwich's refs dict.
                let mut peeled_name = full_ref_name.clone();
                peeled_name.extend_from_slice(b"^{}");
                remote_refs.insert(peeled_name, object.to_hex().to_string());
            }
            Ref::Unborn { .. } => {
                // Empty repo — no objects to record.
            }
        }
    }

    // Early exit: nothing to fetch.
    if wants.is_empty() {
        return Ok(FetchPackResult {
            remote_refs,
            symbolic_refs,
            pack_bytes: vec![],
        });
    }

    // 6. Build fetch arguments.
    let features =
        gix_protocol::Command::Fetch.default_features(actual_protocol, &capabilities);
    let mut args = gix_protocol::fetch::Arguments::new(actual_protocol, features, false);

    for sha in &wants {
        args.want(ObjectId::from_bytes_or_panic(sha));
    }
    for sha in &haves {
        args.have(ObjectId::from_bytes_or_panic(sha));
    }

    // 7. Send DONE immediately (no multi-round negotiation — loader handles this).
    let mut reader = args
        .send(&mut transport, true)
        .context("failed to send fetch arguments")?;

    // 8. Parse ACK/NAK and check whether the server is sending a pack.
    let response =
        gix_protocol::fetch::Response::from_line_reader(actual_protocol, &mut reader, true, false)
            .context("failed to parse fetch response")?;

    // 9. Read pack data.
    //    Enable side-band demultiplexing so the Read impl strips band indicators
    //    and only returns band-1 (pack data), forwarding band-2/3 (progress/error).
    reader.set_progress_handler(Some(Box::new(
        |_is_error: bool, _text: &[u8]| ProgressAction::Continue(()),
    )));

    let mut pack_bytes: Vec<u8> = Vec::new();
    if response.has_pack() {
        if size_limit > 0 {
            // Read up to size_limit bytes then probe for overflow.
            let mut limited = (&mut reader).take(size_limit);
            limited
                .read_to_end(&mut pack_bytes)
                .context("failed to read pack data")?;
            if pack_bytes.len() as u64 == size_limit {
                let mut probe = [0u8; 1];
                let n = reader.read(&mut probe).unwrap_or(0);
                if n > 0 {
                    anyhow::bail!("pack size exceeds limit of {size_limit} bytes");
                }
            }
        } else {
            reader
                .read_to_end(&mut pack_bytes)
                .context("failed to read pack data")?;
        }
    }

    Ok(FetchPackResult {
        remote_refs,
        symbolic_refs,
        pack_bytes,
    })
}

/// Result of a `fetch_pack_to_file` call (pack written to disk, not returned).
pub struct FetchPackFileResult {
    pub remote_refs: HashMap<BString, String>,
    pub symbolic_refs: HashMap<BString, BString>,
    pub pack_size: u64,
}

/// Like [`fetch_pack`] but writes the pack data to `pack_path` on disk
/// instead of returning it in memory.  For large repositories this avoids
/// holding the entire pack in a `Vec<u8>`.
pub fn fetch_pack_to_file(
    url: &str,
    wants: Vec<[u8; 20]>,
    haves: Vec<[u8; 20]>,
    size_limit: u64,
    pack_path: &Path,
    connect_timeout_secs: Option<u64>,
    read_timeout_secs: Option<u64>,
) -> Result<FetchPackFileResult> {
    let url_parsed =
        gix_url::parse(url.into()).with_context(|| format!("invalid git URL: {url}"))?;
    let options = ConnectOptions {
        version: Protocol::V1,
        ..Default::default()
    };
    let mut transport =
        connect(url_parsed, options).with_context(|| format!("failed to connect to {url}"))?;
    apply_timeouts(&mut *transport, connect_timeout_secs, read_timeout_secs)?;

    let (actual_protocol, capabilities, parsed_refs) = {
        let mut outcome = transport
            .handshake(Service::UploadPack, &[])
            .context("git handshake failed")?;
        let actual_protocol = outcome.actual_protocol;
        let (parsed_refs, _) = match outcome.refs.take() {
            Some(mut reader) => {
                gix_protocol::handshake::refs::from_v1_refs_received_as_part_of_handshake_and_capabilities(
                    &mut *reader,
                    outcome.capabilities.iter(),
                )
                .context("failed to parse remote refs")?
            }
            None => (vec![], vec![]),
        };
        let capabilities = outcome.capabilities;
        (actual_protocol, capabilities, parsed_refs)
    };

    let mut remote_refs: HashMap<BString, String> = HashMap::new();
    let mut symbolic_refs: HashMap<BString, BString> = HashMap::new();
    for r in &parsed_refs {
        match r {
            Ref::Direct {
                full_ref_name,
                object,
            } => {
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Symbolic {
                full_ref_name,
                target,
                object,
                ..
            } => {
                symbolic_refs.insert(full_ref_name.clone(), target.clone());
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Peeled {
                full_ref_name,
                tag,
                object,
            } => {
                // Bare ref name → annotated-tag object OID (becomes the SWH
                // Release / snapshot branch target); mirrors dulwich.  See the
                // matching arm in `fetch_pack` for the full rationale.
                remote_refs.insert(full_ref_name.clone(), tag.to_hex().to_string());
                let mut peeled_name = full_ref_name.clone();
                peeled_name.extend_from_slice(b"^{}");
                remote_refs.insert(peeled_name, object.to_hex().to_string());
            }
            Ref::Unborn { .. } => {}
        }
    }

    if wants.is_empty() {
        std::fs::write(pack_path, b"").context("failed to create empty pack file")?;
        return Ok(FetchPackFileResult {
            remote_refs,
            symbolic_refs,
            pack_size: 0,
        });
    }

    let features =
        gix_protocol::Command::Fetch.default_features(actual_protocol, &capabilities);
    let mut args = gix_protocol::fetch::Arguments::new(actual_protocol, features, false);
    for sha in &wants {
        args.want(ObjectId::from_bytes_or_panic(sha));
    }
    for sha in &haves {
        args.have(ObjectId::from_bytes_or_panic(sha));
    }

    let mut reader = args
        .send(&mut transport, true)
        .context("failed to send fetch arguments")?;
    let response =
        gix_protocol::fetch::Response::from_line_reader(actual_protocol, &mut reader, true, false)
            .context("failed to parse fetch response")?;

    reader.set_progress_handler(Some(Box::new(
        |_is_error: bool, _text: &[u8]| ProgressAction::Continue(()),
    )));

    let mut pack_size: u64 = 0;
    if response.has_pack() {
        let mut file = std::fs::File::create(pack_path)
            .with_context(|| format!("failed to create pack file {}", pack_path.display()))?;
        if size_limit > 0 {
            let mut limited = (&mut reader).take(size_limit);
            pack_size = std::io::copy(&mut limited, &mut file)
                .context("failed to write pack data to file")?;
            if pack_size == size_limit {
                let mut probe = [0u8; 1];
                if reader.read(&mut probe).unwrap_or(0) > 0 {
                    drop(file);
                    let _ = std::fs::remove_file(pack_path);
                    anyhow::bail!("pack size exceeds limit of {size_limit} bytes");
                }
            }
        } else {
            pack_size = std::io::copy(&mut reader, &mut file)
                .context("failed to write pack data to file")?;
        }
        file.flush().context("failed to flush pack file")?;
    }

    Ok(FetchPackFileResult {
        remote_refs,
        symbolic_refs,
        pack_size,
    })
}
