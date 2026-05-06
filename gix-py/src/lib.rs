// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! PyO3 bindings exposing swh-loader-git-gix as `swh.loader.git._gix`.

mod exceptions;

use exceptions::{
    map_gix_error, GixFatalError, GixObjectParseError, GixPackError, GixTraverseError,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// Returns the version of the underlying gix library binding.
///
/// Used by tests to verify the extension module loads correctly.
#[pyfunction]
fn version() -> &'static str {
    swh_loader_git_gix::version()
}

/// Fetch a git pack from a remote repository.
///
/// Parameters
/// ----------
/// url : str
///     Remote git URL (http:// or https://).
/// wants : list[bytes]
///     20-byte SHA-1 object IDs to request from the remote.
/// haves : list[bytes]
///     20-byte SHA-1 object IDs we already have (for delta compression hints).
/// size_limit : int
///     Maximum pack size in bytes; 0 means unlimited.
///
/// Returns
/// -------
/// tuple[dict[bytes, str], dict[bytes, bytes], bytes]
///     A 3-tuple of:
///     - ``remote_refs``: mapping ref name → 40-char hex SHA-1 string
///     - ``symbolic_refs``: mapping ref name → target ref name
///     - ``pack_data``: raw pack bytes (empty if nothing to fetch)
///
/// Raises
/// ------
/// ValueError
///     If the URL is invalid, the connection fails, or the pack exceeds
///     ``size_limit``.
#[pyfunction]
#[pyo3(signature = (url, wants, haves, size_limit=0))]
fn fetch_pack<'py>(
    py: Python<'py>,
    url: &str,
    wants: Vec<Vec<u8>>,
    haves: Vec<Vec<u8>>,
    size_limit: u64,
) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>, Bound<'py, PyBytes>)> {
    // Convert Python bytes → [u8; 20]
    let wants: Result<Vec<[u8; 20]>, _> = wants
        .iter()
        .map(|b| {
            <[u8; 20]>::try_from(b.as_slice())
                .map_err(|_| PyValueError::new_err("want SHA-1 must be exactly 20 bytes"))
        })
        .collect();
    let wants = wants?;

    let haves: Result<Vec<[u8; 20]>, _> = haves
        .iter()
        .map(|b| {
            <[u8; 20]>::try_from(b.as_slice())
                .map_err(|_| PyValueError::new_err("have SHA-1 must be exactly 20 bytes"))
        })
        .collect();
    let haves = haves?;

    // Call into pure Rust library
    let result = swh_loader_git_gix::fetch_pack(url, wants, haves, size_limit)
        .map_err(map_gix_error)?;

    // Build remote_refs dict: bytes → str
    let remote_refs_dict = PyDict::new(py);
    for (k, v) in result.remote_refs {
        remote_refs_dict.set_item(PyBytes::new(py, &k), v)?;
    }

    // Build symbolic_refs dict: bytes → bytes
    let symbolic_refs_dict = PyDict::new(py);
    for (k, v) in result.symbolic_refs {
        symbolic_refs_dict.set_item(PyBytes::new(py, &k), PyBytes::new(py, &v))?;
    }

    let pack = PyBytes::new(py, &result.pack_bytes);

    Ok((remote_refs_dict, symbolic_refs_dict, pack))
}

/// Parse and inflate all objects from a raw git pack byte stream.
///
/// Parameters
/// ----------
/// pack_bytes : bytes
///     A complete pack file (PACK header + objects + trailer).  Non-thin packs only.
///
/// Returns
/// -------
/// list[tuple[int, bytes, bytes]]
///     One tuple per object: ``(type_num, sha1_20bytes, raw_data)``.
///     ``type_num`` follows dulwich convention: Commit=1, Tree=2, Blob=3, Tag=4.
///     Delta chains are fully resolved; every object is ready to be stored.
///
/// Raises
/// ------
/// ValueError
///     If the pack is corrupt, truncated, or a decompression error occurs.
#[pyfunction]
fn iter_pack_objects<'py>(
    py: Python<'py>,
    pack_bytes: &[u8],
) -> PyResult<Vec<(u8, Bound<'py, PyBytes>, Bound<'py, PyBytes>)>> {
    let objects = swh_loader_git_gix::iter_pack_objects(pack_bytes)
        .map_err(map_gix_error)?;

    objects
        .into_iter()
        .map(|obj| {
            Ok((
                obj.type_num,
                PyBytes::new(py, &obj.sha1),
                PyBytes::new(py, &obj.data),
            ))
        })
        .collect()
}

/// Parse and inflate a pack, returning objects partitioned by type.
///
/// Parameters
/// ----------
/// pack_bytes : bytes
///     A complete pack file (PACK header + objects + trailer).  Non-thin packs only.
///
/// Returns
/// -------
/// tuple[list, list, list, list]
///     A 4-tuple of:
///     - ``blobs``:   list of ``(sha1_git, sha1, sha256, blake2s256, data)`` — all bytes
///     - ``trees``:   list of ``(sha1_git, raw_data, entries)`` where entries is
///       ``list[(mode_u32, name_bytes, target_sha1_bytes)]``
///     - ``commits``: list of ``(sha1_git, raw_data)``
///     - ``tags``:    list of ``(sha1_git, raw_data)``
///
///     For blobs, all four hash algorithms (sha1, sha1_git, sha256, blake2s256) are
///     computed in Rust.  For trees, the binary format is parsed in Rust.  For
///     commits/tags, raw bytes are returned for Python-side parsing.
///
/// Raises
/// ------
/// ValueError
///     If the pack is corrupt, truncated, or a decompression error occurs.
#[pyfunction]
fn inflate_types<'py>(
    py: Python<'py>,
    pack_bytes: &[u8],
) -> PyResult<(
    // blobs
    Vec<(
        Bound<'py, PyBytes>,
        Bound<'py, PyBytes>,
        Bound<'py, PyBytes>,
        Bound<'py, PyBytes>,
        Bound<'py, PyBytes>,
    )>,
    // trees
    Vec<(
        Bound<'py, PyBytes>,
        Bound<'py, PyBytes>,
        Vec<(u32, Bound<'py, PyBytes>, Bound<'py, PyBytes>)>,
    )>,
    // commits
    Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)>,
    // tags
    Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)>,
)> {
    let result = swh_loader_git_gix::inflate_pack_typed(pack_bytes)
        .map_err(map_gix_error)?;

    let blobs = result
        .blobs
        .into_iter()
        .map(|b| {
            (
                PyBytes::new(py, &b.sha1_git),
                PyBytes::new(py, &b.sha1),
                PyBytes::new(py, &b.sha256),
                PyBytes::new(py, &b.blake2s256),
                PyBytes::new(py, &b.data),
            )
        })
        .collect();

    let trees = result
        .trees
        .into_iter()
        .map(|t| {
            let entries: Vec<_> = t
                .entries
                .into_iter()
                .map(|e| (e.mode, PyBytes::new(py, &e.name), PyBytes::new(py, &e.sha1)))
                .collect();
            (PyBytes::new(py, &t.sha1_git), PyBytes::new(py, &t.raw_data), entries)
        })
        .collect();

    let commits = result
        .commits
        .into_iter()
        .map(|c| (PyBytes::new(py, &c.sha1_git), PyBytes::new(py, &c.data)))
        .collect();

    let tags = result
        .tags
        .into_iter()
        .map(|t| (PyBytes::new(py, &t.sha1_git), PyBytes::new(py, &t.data)))
        .collect();

    Ok((blobs, trees, commits, tags))
}

/// Fetch a git pack and write it to a file on disk (streaming, O(1) memory).
///
/// Parameters
/// ----------
/// url : str
///     Remote git URL.
/// wants : list[bytes]
///     20-byte SHA-1 object IDs to request.
/// haves : list[bytes]
///     20-byte SHA-1 object IDs we already have.
/// size_limit : int
///     Maximum pack size in bytes; 0 means unlimited.
/// pack_path : str
///     File path to write the pack data to.
///
/// Returns
/// -------
/// tuple[dict[bytes, str], dict[bytes, bytes], int]
///     ``(remote_refs, symbolic_refs, pack_size_bytes)``
#[pyfunction]
#[pyo3(signature = (url, wants, haves, size_limit, pack_path))]
fn fetch_pack_to_file<'py>(
    py: Python<'py>,
    url: &str,
    wants: Vec<Vec<u8>>,
    haves: Vec<Vec<u8>>,
    size_limit: u64,
    pack_path: &str,
) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>, u64)> {
    let wants: Result<Vec<[u8; 20]>, _> = wants
        .iter()
        .map(|b| {
            <[u8; 20]>::try_from(b.as_slice())
                .map_err(|_| PyValueError::new_err("want SHA-1 must be exactly 20 bytes"))
        })
        .collect();
    let wants = wants?;
    let haves: Result<Vec<[u8; 20]>, _> = haves
        .iter()
        .map(|b| {
            <[u8; 20]>::try_from(b.as_slice())
                .map_err(|_| PyValueError::new_err("have SHA-1 must be exactly 20 bytes"))
        })
        .collect();
    let haves = haves?;

    let result = swh_loader_git_gix::fetch_pack_to_file(
        url,
        wants,
        haves,
        size_limit,
        std::path::Path::new(pack_path),
    )
    .map_err(map_gix_error)?;

    let remote_refs_dict = PyDict::new(py);
    for (k, v) in result.remote_refs {
        remote_refs_dict.set_item(PyBytes::new(py, &k), v)?;
    }
    let symbolic_refs_dict = PyDict::new(py);
    for (k, v) in result.symbolic_refs {
        symbolic_refs_dict.set_item(PyBytes::new(py, &k), PyBytes::new(py, &v))?;
    }
    Ok((remote_refs_dict, symbolic_refs_dict, result.pack_size))
}

/// Streaming iterator over objects in a pack file on disk.
///
/// Memory usage is O(largest single object), not O(total pack).
/// Each call to ``__next__`` yields a tuple whose first element is the
/// type number (1=commit, 2=tree, 3=blob, 4=tag) and remaining elements
/// depend on the type.
/// Mask/constant for commit-type entries (submodules).
const COMMIT_MODE_MASK: u32 = 0o160000;
/// Mask/constant for tree-type entries (directories).
const TREE_MODE_MASK: u32 = 0o040000;

#[pyclass]
struct PackReader {
    inner: swh_loader_git_gix::PackIterator,
    de_cls: PyObject,
    dir_cls: PyObject,
}

#[pymethods]
impl PackReader {
    #[new]
    fn new(py: Python<'_>, pack_path: &str) -> PyResult<Self> {
        let inner = swh_loader_git_gix::PackIterator::open(std::path::Path::new(pack_path))
            .map_err(map_gix_error)?;
        let model = py.import("swh.model.model")?;
        let de_cls = model.getattr("DirectoryEntry")?.unbind();
        let dir_cls = model.getattr("Directory")?.unbind();
        Ok(PackReader {
            inner,
            de_cls,
            dir_cls,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        use swh_loader_git_gix::TypedObject;
        match self.inner.next_object() {
            Ok(None) => Ok(None),
            Err(e) => Err(map_gix_error(e)),
            Ok(Some(obj)) => {
                let tuple = match obj {
                    TypedObject::Blob {
                        sha1_git,
                        sha1,
                        sha256,
                        blake2s256,
                        data,
                    } => (
                        3u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &sha1),
                        PyBytes::new(py, &sha256),
                        PyBytes::new(py, &blake2s256),
                        PyBytes::new(py, &data),
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    TypedObject::Tree {
                        sha1_git,
                        raw_data,
                        entries,
                        hash_match,
                    } => {
                        if hash_match {
                            // Fast path: create Directory via __new__ + object.__setattr__,
                            // bypassing attrs frozen-class validators (data verified by Rust).
                            let de_cls = self.de_cls.bind(py);
                            let dir_cls = self.dir_cls.bind(py);
                            let obj_sa = py.import("builtins")?.getattr("object")?
                                .getattr("__setattr__")?;

                            let mut sorted_entries = entries;
                            sorted_entries.sort_by(|a, b| {
                                let is_dir_a = (a.mode & TREE_MODE_MASK == TREE_MODE_MASK)
                                    && (a.mode & COMMIT_MODE_MASK != COMMIT_MODE_MASK);
                                let is_dir_b = (b.mode & TREE_MODE_MASK == TREE_MODE_MASK)
                                    && (b.mode & COMMIT_MODE_MASK != COMMIT_MODE_MASK);
                                let mut ka = a.name.clone();
                                if is_dir_a { ka.push(b'/'); }
                                let mut kb = b.name.clone();
                                if is_dir_b { kb.push(b'/'); }
                                ka.cmp(&kb)
                            });

                            let mut py_entries = Vec::with_capacity(sorted_entries.len());
                            for e in &sorted_entries {
                                let type_str = if e.mode & COMMIT_MODE_MASK == COMMIT_MODE_MASK {
                                    "rev"
                                } else if e.mode & TREE_MODE_MASK == TREE_MODE_MASK {
                                    "dir"
                                } else {
                                    "file"
                                };
                                let name: Vec<u8> = e.name.iter()
                                    .map(|&b| if b == b'/' { b'_' } else { b })
                                    .collect();

                                let de = de_cls.getattr("__new__")?.call1((de_cls,))?;
                                obj_sa.call1((&de, "name", PyBytes::new(py, &name)))?;
                                obj_sa.call1((&de, "type", type_str))?;
                                obj_sa.call1((&de, "target", PyBytes::new(py, &e.sha1)))?;
                                obj_sa.call1((&de, "perms", e.mode))?;
                                py_entries.push(de.unbind());
                            }

                            let entries_tuple = pyo3::types::PyTuple::new(py, &py_entries)?;
                            let dir_obj = dir_cls.getattr("__new__")?.call1((dir_cls,))?;
                            obj_sa.call1((&dir_obj, "entries", entries_tuple))?;
                            obj_sa.call1((&dir_obj, "id", PyBytes::new(py, &sha1_git)))?;
                            obj_sa.call1((&dir_obj, "raw_manifest", py.None()))?;

                            (2u8, dir_obj.into_any()).into_pyobject(py)?
                                .into_any().unbind()
                        } else {
                            // Slow path: return tuple for Python-side conversion
                            let py_entries: Vec<(u32, Bound<'_, PyBytes>, Bound<'_, PyBytes>)> =
                                entries
                                    .into_iter()
                                    .map(|e| {
                                        (e.mode, PyBytes::new(py, &e.name), PyBytes::new(py, &e.sha1))
                                    })
                                    .collect();
                            (
                                2u8,
                                PyBytes::new(py, &sha1_git),
                                PyBytes::new(py, &raw_data),
                                py_entries,
                                false,
                            )
                                .into_pyobject(py)?
                                .into_any()
                                .unbind()
                        }
                    }
                    TypedObject::Commit {
                        sha1_git,
                        data,
                        hash_match,
                    } => (
                        1u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &data),
                        hash_match,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    TypedObject::Tag {
                        sha1_git,
                        data,
                        hash_match,
                    } => (
                        4u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &data),
                        hash_match,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                };
                Ok(Some(tuple))
            }
        }
    }
}

/// Parallel streaming iterator over objects in a pack file on disk.
///
/// Uses gix-pack's delta-tree traversal to inflate objects across
/// multiple threads.  The Python iterator releases the GIL while
/// waiting for the next object, allowing other Python threads to run.
///
/// Returns the same tuple format as ``PackReader``.
#[pyclass]
struct ParallelPackReader {
    inner: swh_loader_git_gix::ParallelInflater,
}

#[pymethods]
impl ParallelPackReader {
    #[new]
    #[pyo3(signature = (pack_path, channel_bound=None))]
    fn new(_py: Python<'_>, pack_path: &str, channel_bound: Option<usize>) -> PyResult<Self> {
        let bound = channel_bound.unwrap_or(65536);
        let inner =
            swh_loader_git_gix::ParallelInflater::open(std::path::Path::new(pack_path), bound)
                .map_err(map_gix_error)?;
        Ok(ParallelPackReader { inner })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        use swh_loader_git_gix::TypedObject;

        // Release GIL while waiting for the next object from the channel.
        let obj_result = {
            let inner = &self.inner;
            py.allow_threads(|| inner.next_object())
        };

        match obj_result {
            Ok(None) => Ok(None),
            Err(e) => Err(map_gix_error(e)),
            Ok(Some(obj)) => {
                let tuple = match obj {
                    TypedObject::Blob {
                        sha1_git,
                        sha1,
                        sha256,
                        blake2s256,
                        data,
                    } => (
                        3u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &sha1),
                        PyBytes::new(py, &sha256),
                        PyBytes::new(py, &blake2s256),
                        PyBytes::new(py, &data),
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    TypedObject::Tree {
                        sha1_git,
                        raw_data,
                        entries,
                        hash_match,
                    } => {
                        // Always send as a tuple-of-tuples.  Building
                        // Directory/DirectoryEntry Python objects here
                        // dominated the consumer (73% per exp3) because
                        // each tree allocates ~330 attrs instances under
                        // the GIL.  Defer construction to the Python
                        // flush path, which amortises the cost across
                        // batches that overlap with storage I/O.
                        let py_entries: Vec<(
                            u32,
                            Bound<'_, PyBytes>,
                            Bound<'_, PyBytes>,
                        )> = entries
                            .into_iter()
                            .map(|e| {
                                (
                                    e.mode,
                                    PyBytes::new(py, &e.name),
                                    PyBytes::new(py, &e.sha1),
                                )
                            })
                            .collect();
                        (
                            2u8,
                            PyBytes::new(py, &sha1_git),
                            PyBytes::new(py, &raw_data),
                            py_entries,
                            hash_match,
                        )
                            .into_pyobject(py)?
                            .into_any()
                            .unbind()
                    }
                    TypedObject::Commit {
                        sha1_git,
                        data,
                        hash_match,
                    } => (
                        1u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &data),
                        hash_match,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    TypedObject::Tag {
                        sha1_git,
                        data,
                        hash_match,
                    } => (
                        4u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &data),
                        hash_match,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                };
                Ok(Some(tuple))
            }
        }
    }
}

/// Direct delta-tree parallel iterator (no `git index-pack` needed).
///
/// Builds the delta dependency tree from a streaming header scan, then
/// traverses in parallel — eliminating the `git index-pack` subprocess
/// and the `.idx` file entirely.
///
/// Returns the same tuple format as ``ParallelPackReader``.
///
/// Only supports packs with OFS_DELTA references (standard for non-thin
/// packs from ``git upload-pack``).  Raises ``ValueError`` if a
/// REF_DELTA is encountered.
#[pyclass]
struct DirectTreePackReader {
    inner: swh_loader_git_gix::DirectTreeInflater,
}

#[pymethods]
impl DirectTreePackReader {
    #[new]
    #[pyo3(signature = (pack_path, channel_bound=None))]
    fn new(_py: Python<'_>, pack_path: &str, channel_bound: Option<usize>) -> PyResult<Self> {
        let bound = channel_bound.unwrap_or(65536);
        let inner = swh_loader_git_gix::DirectTreeInflater::open(
            std::path::Path::new(pack_path),
            bound,
        )
        .map_err(map_gix_error)?;
        Ok(DirectTreePackReader { inner })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        use swh_loader_git_gix::TypedObject;

        // Release GIL while waiting for the next object from the channel.
        let obj_result = {
            let inner = &self.inner;
            py.allow_threads(|| inner.next_object())
        };

        match obj_result {
            Ok(None) => Ok(None),
            Err(e) => Err(map_gix_error(e)),
            Ok(Some(obj)) => {
                let tuple = match obj {
                    TypedObject::Blob {
                        sha1_git,
                        sha1,
                        sha256,
                        blake2s256,
                        data,
                    } => (
                        3u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &sha1),
                        PyBytes::new(py, &sha256),
                        PyBytes::new(py, &blake2s256),
                        PyBytes::new(py, &data),
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    TypedObject::Tree {
                        sha1_git,
                        raw_data,
                        entries,
                        hash_match,
                    } => {
                        // Always send as a tuple-of-tuples.  Building
                        // Directory/DirectoryEntry Python objects here
                        // dominated the consumer (73% per exp3) because
                        // each tree allocates ~330 attrs instances under
                        // the GIL.  Defer construction to the Python
                        // flush path, which amortises the cost across
                        // batches that overlap with storage I/O.
                        let py_entries: Vec<(
                            u32,
                            Bound<'_, PyBytes>,
                            Bound<'_, PyBytes>,
                        )> = entries
                            .into_iter()
                            .map(|e| {
                                (
                                    e.mode,
                                    PyBytes::new(py, &e.name),
                                    PyBytes::new(py, &e.sha1),
                                )
                            })
                            .collect();
                        (
                            2u8,
                            PyBytes::new(py, &sha1_git),
                            PyBytes::new(py, &raw_data),
                            py_entries,
                            hash_match,
                        )
                            .into_pyobject(py)?
                            .into_any()
                            .unbind()
                    }
                    TypedObject::Commit {
                        sha1_git,
                        data,
                        hash_match,
                    } => (
                        1u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &data),
                        hash_match,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    TypedObject::Tag {
                        sha1_git,
                        data,
                        hash_match,
                    } => (
                        4u8,
                        PyBytes::new(py, &sha1_git),
                        PyBytes::new(py, &data),
                        hash_match,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                };
                Ok(Some(tuple))
            }
        }
    }
}

#[pymodule]
#[pyo3(name = "_gix")]
mod _gix {
    use super::*;

    #[pymodule_export]
    use super::fetch_pack;
    #[pymodule_export]
    use super::fetch_pack_to_file;
    #[pymodule_export]
    use super::inflate_types;
    #[pymodule_export]
    use super::iter_pack_objects;
    #[pymodule_export]
    use super::version;
    #[pymodule_export]
    use super::PackReader;
    #[pymodule_export]
    use super::ParallelPackReader;
    #[pymodule_export]
    use super::DirectTreePackReader;

    // Workaround for https://github.com/PyO3/pyo3/issues/759:
    // register the module under its full dotted name so it is importable as
    // `from swh.loader.git._gix import ...`.
    // Also register the typed exception classes as module attributes so
    // callers can `from swh.loader.git._gix import GixPackError` etc.
    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        m.add("GixPackError", py.get_type::<GixPackError>())?;
        m.add("GixObjectParseError", py.get_type::<GixObjectParseError>())?;
        m.add("GixTraverseError", py.get_type::<GixTraverseError>())?;
        m.add("GixFatalError", py.get_type::<GixFatalError>())?;

        py.import("sys")?
            .getattr("modules")?
            .set_item("swh.loader.git._gix", m)
    }
}
