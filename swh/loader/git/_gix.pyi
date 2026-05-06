"""Type stubs for the swh.loader.git._gix compiled extension (gitoxide bindings)."""

# Typed exception classes raised by the gix bindings. See
# ``gix-py/src/exceptions.rs`` and
# ``notes/DESIGN-dulwich-fallback-signals.md`` in the swh-environment audit
# tree for the design rationale.
class GixPackError(ValueError):
    """Pack-format error; fallback to dulwich may succeed."""

class GixObjectParseError(ValueError):
    """Object-level parsing error (malformed tree, bad mode bits); fallback OK."""

class GixTraverseError(ValueError):
    """Parallel-traverse / DirectTreeInflater scan error; fallback may succeed."""

class GixFatalError(ValueError):
    """Fatal for both backends (auth, network, fs, policy); do not fall back."""

def version() -> str:
    """Return the version of the swh-loader-git-gix Rust library."""
    ...

def fetch_pack(
    url: str,
    wants: list[bytes],
    haves: list[bytes],
    size_limit: int = 0,
) -> tuple[dict[bytes, str], dict[bytes, bytes], bytes]:
    """Fetch a git pack from a remote repository over HTTP/HTTPS.

    Parameters
    ----------
    url:
        Remote git URL (``http://`` or ``https://``).
    wants:
        20-byte SHA-1 object IDs to request from the remote.
    haves:
        20-byte SHA-1 object IDs we already have (for delta compression hints).
    size_limit:
        Maximum pack size in bytes; 0 means unlimited.

    Returns
    -------
    tuple[dict[bytes, str], dict[bytes, bytes], bytes]
        A 3-tuple of:

        - *remote_refs* — mapping of ref name (bytes) → 40-char hex SHA-1 (str)
        - *symbolic_refs* — mapping of ref name (bytes) → target ref name (bytes)
        - *pack_data* — raw pack bytes (empty bytes if nothing to fetch)

    Raises
    ------
    ValueError
        If the URL is invalid, the connection fails, or the pack exceeds
        ``size_limit``.
    """
    ...

def iter_pack_objects(
    pack_bytes: bytes,
) -> list[tuple[int, bytes, bytes]]:
    """Parse and inflate all objects from a raw git pack byte stream.

    Parameters
    ----------
    pack_bytes:
        A complete pack file (PACK header + objects + trailer).  Non-thin packs
        only; the caller is responsible for ensuring no external ref-delta bases
        are needed.

    Returns
    -------
    list[tuple[int, bytes, bytes]]
        One tuple per object in pack order:

        - *type_num* (``int``) — dulwich-compatible type: Commit=1, Tree=2,
          Blob=3, Tag=4.
        - *sha1* (``bytes``) — 20-byte raw SHA-1 of the object.
        - *raw_data* (``bytes``) — uncompressed object data.

        Delta chains are fully resolved; every object is ready to be stored.

    Raises
    ------
    ValueError
        If the pack is corrupt, truncated, or a decompression error occurs.
    """
    ...

def fetch_pack_to_file(
    url: str,
    wants: list[bytes],
    haves: list[bytes],
    size_limit: int,
    pack_path: str,
) -> tuple[dict[bytes, str], dict[bytes, bytes], int]:
    """Fetch a git pack and write it to a file on disk (streaming, O(1) memory)."""
    ...

class PackReader:
    """Streaming iterator over objects in a pack file on disk."""

    def __init__(self, pack_path: str) -> None: ...
    def __iter__(self) -> "PackReader": ...
    def __next__(self) -> tuple: ...

class ParallelPackReader:
    """Parallel streaming iterator using multiple worker threads."""

    def __init__(self, pack_path: str, channel_bound: int | None = None) -> None: ...
    def __iter__(self) -> "ParallelPackReader": ...
    def __next__(self) -> tuple: ...
