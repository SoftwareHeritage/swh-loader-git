# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io
import logging
import os
import tarfile
import tempfile
from typing import Any, Dict, Optional

from .loader import GitLoader

logger = logging.getLogger(__name__)


class GitTarOutLoader(GitLoader):
    """Git loader that stores the fetched repository in an uncompressed tar archive.

    If `generate_index` is True (the default), the archive is compatible with a
    `git clone --bare` result. Otherwise, the index file (.idx) is missing and
    the repository is not immediately functional. It can be generated manually
    later using `git index-pack objects/pack/pack-<HASH>.pack`.

    This loader does not load the data into the Software Heritage storage.
    """

    def __init__(
        self,
        *args,
        tar_output_path: Optional[str] = None,
        generate_index: bool = True,
        prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.tar_output_path = tar_output_path
        self.generate_index = generate_index

        if prefix is None:
            # Extract project name from URL
            prefix = self.origin.url.rstrip("/").split("/")[-1]
            if prefix.endswith(".git"):
                prefix = prefix[:-4]

        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self.prefix = prefix

    def store_data(self) -> None:
        """Create the tar archive containing the bare git repository."""
        if not self.tar_output_path:
            logger.warning("No tar_output_path provided, skipping tar creation.")
            return

        self._create_tar()

    def _create_tar(self) -> None:
        assert self.tar_output_path is not None

        if not self.pack_data:
            logger.info("No pack data to store.")
            return

        # The SHA-1 is the last 20 bytes of the pack file. We need it to name
        # the pack file and its index in the tar archive, as expected by Git
        # for bare repositories. We also need it to generate the index file
        # correctly.
        pack_sha1_bytes = self.pack_data.get_stored_checksum()
        pack_hash_hex = pack_sha1_bytes.hex()

        # Generate the pack index on the fly.
        # Note: The Git protocol only transfers the pack file (.pack); the
        # index file (.idx) must be generated locally for the repository to
        # be functional.
        # Optimization: One could skip/delay this if only the raw pack is needed.
        tmp_idx_path = None
        index_size = 0
        if self.generate_index:
            logger.info("Generating Git index ...")
            # dulwich create_index requires a filename on disk.
            with tempfile.NamedTemporaryFile(suffix=".idx", delete=False) as tmp_idx:
                tmp_idx_path = tmp_idx.name
            try:
                self.pack_data.create_index(tmp_idx_path)
                index_size = os.path.getsize(tmp_idx_path)

                logger.info("Creating tar archive at %s", self.tar_output_path)
                # Use streaming mode "w|" for efficiency.
                with tarfile.open(self.tar_output_path, mode="w|") as tar:
                    self._fill_tar(tar, pack_hash_hex, tmp_idx_path, index_size)
            finally:
                if tmp_idx_path and os.path.exists(tmp_idx_path):
                    os.remove(tmp_idx_path)
        else:
            logger.info("Creating tar archive at %s (skipping index)...", self.tar_output_path)
            with tarfile.open(self.tar_output_path, mode="w|") as tar:
                self._fill_tar(tar, pack_hash_hex)

    def _fill_tar(
        self,
        tar: tarfile.TarFile,
        pack_hash_hex: str,
        tmp_idx_path: Optional[str] = None,
        index_size: int = 0,
    ) -> None:
        # HEAD
        head_ref = self.symbolic_refs.get(b"HEAD", b"refs/heads/master")
        head_content = b"ref: " + head_ref + b"\n"
        self._add_to_tar(tar, f"{self.prefix}HEAD", head_content)

        # config
        config_content = (
            b"[core]\n"
            b"\trepositoryformatversion = 0\n"
            b"\tfilemode = true\n"
            b"\tbare = true\n"
        )
        self._add_to_tar(tar, f"{self.prefix}config", config_content)

        # description
        description_content = (
            b"Unnamed repository; edit this file 'description' to name the "
            b"repository.\n"
        )
        self._add_to_tar(tar, f"{self.prefix}description", description_content)

        # refs
        for ref_name, ref_sha in self.remote_refs.items():
            if ref_name.startswith(b"refs/heads/") or ref_name.startswith(
                b"refs/tags/"
            ):
                self._add_to_tar(
                    tar, f"{self.prefix}{ref_name.decode()}", ref_sha + b"\n"
                )

        # objects/pack/pack-<hash>.pack
        if self.pack_size > 0:
            self.pack_buffer.seek(0)
            pack_path = f"{self.prefix}objects/pack/pack-{pack_hash_hex}.pack"
            tar_info = tarfile.TarInfo(name=pack_path)
            tar_info.size = self.pack_size
            tar.addfile(tar_info, fileobj=self.pack_buffer)

            # objects/pack/pack-<hash>.idx
            if tmp_idx_path:
                with open(tmp_idx_path, "rb") as index_f:
                    index_path = f"{self.prefix}objects/pack/pack-{pack_hash_hex}.idx"
                    tar_info = tarfile.TarInfo(name=index_path)
                    tar_info.size = index_size
                    tar.addfile(tar_info, fileobj=index_f)

    def _add_to_tar(self, tar: tarfile.TarFile, name: str, content: bytes) -> None:
        tar_info = tarfile.TarInfo(name=name)
        tar_info.size = len(content)
        tar.addfile(tar_info, fileobj=io.BytesIO(content))

    def load_status(self) -> Dict[str, Any]:
        """The load was eventful if a pack was fetched."""
        return {"status": "eventful" if self.pack_size > 0 else "uneventful"}
