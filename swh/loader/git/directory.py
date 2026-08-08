# Copyright (C) 2023-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from os.path import basename, exists, join
from pathlib import Path
import selectors
from shutil import which
from subprocess import (
    DEVNULL,
    PIPE,
    CalledProcessError,
    Popen,
    TimeoutExpired,
    check_output,
)
import tempfile
from time import monotonic
from typing import Any, Iterable, Iterator, Optional

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.exception import NotFound
from swh.loader.git.utils import raise_not_found_repository
from swh.model.from_disk import ignore_empty_directories, ignore_named_directories
from swh.model.model import Snapshot, SnapshotBranch, SnapshotTargetType

logger = logging.getLogger(__name__)

# Maximum number of entries, one per inode a checkout creates, that a tree may
# expand to. A tree whose entries repeatedly name the same subtree is a few
# kilobytes on the wire and unbounded on disk.
DEFAULT_MAX_TREE_ENTRIES = 10_000_000

# Wall-clock limit for any single git subprocess.
DEFAULT_GIT_TIMEOUT_SECONDS = 3600

# Size of the chunks ``git ls-tree`` output is drained in.
LS_TREE_READ_SIZE = 1 << 16


class TreeTooLarge(ValueError):
    """The ref's tree expands to more entries than the configured limit.

    Raised before anything is written to disk.
    """


def git() -> str:
    """Get the path to the git executable.

    Raises:
        EnvironmentError if no opam executable is found
    """
    ret = which("git")
    if not ret:
        raise EnvironmentError("No git executable found in path {os.environ['PATH']}")

    return ret


def resolve_ref(local_path: str, ref: str, timeout: Optional[int] = None) -> str:
    """Resolve ``ref`` to the commit id it denotes, the way ``git checkout`` does.

    ``git checkout <name>`` falls back to the remote-tracking branch
    ``origin/<name>`` when no local branch of that name exists; plumbing
    commands such as ``git ls-tree`` do not. Resolving here also hands
    ``git ls-tree`` a hexadecimal id rather than a remote-controlled string.

    Args:
        local_path: the repository working directory
        ref: commit, tag or branch name
        timeout: wall-clock limit for each git subprocess

    Returns:
        the commit id ``ref`` resolves to

    Raises:
        CalledProcessError: if ``ref`` cannot be resolved
        TimeoutExpired: if git does not answer within ``timeout``
    """
    error: Optional[CalledProcessError] = None
    for candidate in (ref, f"origin/{ref}"):
        try:
            return (
                check_output(
                    [git(), "rev-parse", "--verify", f"{candidate}^{{commit}}"],
                    cwd=local_path,
                    env={"LC_ALL": "C"},
                    stderr=PIPE,
                    timeout=timeout,
                )
                .decode()
                .strip()
            )
        except CalledProcessError as cpe:
            # report the failure for the ref as it was asked for, not for the
            # `origin/` spelling we tried on its behalf
            error = error or cpe
    assert error is not None
    raise error


def count_tree_entries(
    local_path: str, ref: str, limit: int, timeout: Optional[int] = None
) -> int:
    """Count the entries ``ref``'s tree expands to, without materialising it.

    ``git ls-tree -r -t`` emits one record per blob, per gitlink and per
    directory, which is one per inode a checkout creates. ``-t`` is what makes
    the count inodes rather than files, so it must stay: three tests fail
    without it.

    It streams, because git's traversal is depth-first with a bounded stack, so
    reading stops and the subprocess is killed as soon as the count exceeds
    ``limit``: a refusal costs O(``limit``) and touches no disk.

    ``-z`` makes the output NUL-terminated, one NUL per record, and NUL is the
    one byte a pathname cannot contain, so counting NUL bytes is exact whatever
    the names are.

    Args:
        local_path: the repository working directory
        ref: the revision to inspect; pass a resolved commit id, see
            :func:`resolve_ref`
        limit: stop and raise once strictly more than this many entries are seen
        timeout: wall-clock limit for reaping the subprocess

    Returns:
        the exact number of entries, when it is within ``limit``

    Raises:
        TreeTooLarge: if the tree expands to more than ``limit`` entries
        CalledProcessError: if git itself fails
    """
    cmd = [git(), "ls-tree", "-r", "-t", "-z", "--name-only", ref]
    # stderr to /dev/null, not to a second pipe: nothing drains it while we
    # read stdout, so git could block on it.
    proc = Popen(cmd, cwd=local_path, stdout=PIPE, stderr=DEVNULL, env={"LC_ALL": "C"})
    assert proc.stdout is not None
    count = 0

    # Bounded read: a git that goes quiet without exiting must not block us,
    # and a short count would be an undercount. The raw descriptor, not the
    # buffered reader, so os.read() returns what has arrived.
    selector = selectors.DefaultSelector()
    stdout_fd = proc.stdout.fileno()
    selector.register(stdout_fd, selectors.EVENT_READ)
    started = monotonic()

    try:
        while True:
            if timeout is not None:
                left = timeout - (monotonic() - started)
                if left <= 0 or not selector.select(left):
                    raise TimeoutExpired(cmd, timeout)
            chunk = os.read(stdout_fd, LS_TREE_READ_SIZE)
            if not chunk:
                break
            count += chunk.count(b"\0")
            if count > limit:
                raise TreeTooLarge(
                    f"tree of {ref} expands to more than {limit} entries; "
                    f"refusing to check it out"
                )
    except BaseException:
        # leaving without draining the pipe: git may be blocked writing to it
        proc.kill()
        raise
    finally:
        selector.close()
        # git has either closed stdout or been killed, so this returns promptly
        proc.stdout.close()
        try:
            proc.wait(timeout=timeout)
        except TimeoutExpired:  # pragma: no cover - a killed git does exit
            proc.kill()
            proc.wait()
    # only reachable when the loop ran to EOF
    if proc.returncode != 0:
        raise CalledProcessError(proc.returncode, cmd)
    return count


def checkout_repository_ref(
    git_url: str,
    git_ref: str,
    target: Path,
    max_tree_entries: int = DEFAULT_MAX_TREE_ENTRIES,
    timeout: Optional[int] = DEFAULT_GIT_TIMEOUT_SECONDS,
) -> Path:
    """Checkout the reference ``git_ref`` (commit, tag or branch) from git repository
    located at ``git_url``.

    The tree is counted from the fetched objects before it is materialised,
    and the checkout is skipped when it is too large. See
    :func:`count_tree_entries`.

    This function can raise for various reasons. This is expected to be caught by the
    main loop in the loader.

    Args:
        git_url: the remote repository
        git_ref: commit, tag or branch to check out
        target: directory to clone into
        max_tree_entries: refuse to check out a tree expanding beyond this
        timeout: wall-clock limit applied to each git subprocess

    Raises:
        TreeTooLarge: the ref's tree expands past ``max_tree_entries``
        NotFound: the remote repository is unreachable
        TimeoutExpired: a git subprocess overran ``timeout``; deliberately not
            caught here, so the visit fails rather than being reported
            ``not_found``.
    """

    local_name = basename(git_url.rstrip("/"))
    local_path = str(target / local_name)
    os.mkdir(local_path)

    def run_git_cmd(cmd):
        # ensure english output
        env = {"LC_ALL": "C"}
        check_output(
            [git()] + cmd, cwd=local_path, env=env, stderr=PIPE, timeout=timeout
        )

    try:
        run_git_cmd(["init", "--initial-branch=main"])
        run_git_cmd(["remote", "add", "origin", git_url])
        try:
            run_git_cmd(["fetch", "--depth", "1", "origin", git_ref])
        except CalledProcessError:
            # shallow fetch failed, retry a full one
            run_git_cmd(["fetch", "-t", "origin"])
            checkout_ref = git_ref
        else:
            checkout_ref = "FETCH_HEAD"

        # count before materialising: everything below this line is the cost
        entries = count_tree_entries(
            local_path,
            resolve_ref(local_path, checkout_ref, timeout=timeout),
            limit=max_tree_entries,
            timeout=timeout,
        )
        logger.debug(
            "%s@%s expands to %d entries (limit %d)",
            git_url,
            git_ref,
            entries,
            max_tree_entries,
        )
        run_git_cmd(["checkout", checkout_ref])

    except CalledProcessError as cpe:
        # only run_git_cmd captures stderr; count_tree_entries raises with none
        if cpe.stderr and b"fatal: Could not read from remote repository" in cpe.stderr:
            raise NotFound(f"Repository <{git_url}> not found")
        raise

    return Path(local_path)


def list_git_tree(dirpath: bytes, dirname: bytes, entries: Iterable[Any]) -> bool:
    # def list_git_tree() -> Callable:
    """List a git tree. This ignores any repo_path/.git/* and empty folders. This is a
    filter for :func:`directory_to_objects` to ignore specific directories.

    """
    return ignore_named_directories([b".git"])(
        dirpath, dirname, entries
    ) and ignore_empty_directories(dirpath, dirname, entries)


class GitCheckoutLoader(BaseDirectoryLoader):
    """Git directory loader in charge of ingesting a git tree at a specific commit, tag
    or branch into the swh archive.

    As per the standard git hash computations, this ignores the .git and the empty
    directories.

    The output snapshot is of the form:

    .. code::

       id: <bytes>
       branches:
         HEAD:
           target_type: alias
           target: <git-ref>
         <git-ref>:
           target_type: directory
           target: <directory-id>

    """

    visit_type = "git-checkout"

    def __init__(self, *args, **kwargs):
        self.git_ref = kwargs.pop("ref")
        self.submodules = kwargs.pop("submodules", False)
        # coerced like `max_content_size` is in BaseLoader, as both may reach us
        # as strings from a configuration file
        self.max_tree_entries = int(
            kwargs.pop("max_tree_entries", DEFAULT_MAX_TREE_ENTRIES)
        )
        git_timeout = kwargs.pop("git_timeout", DEFAULT_GIT_TIMEOUT_SECONDS)
        self.git_timeout = int(git_timeout) if git_timeout else None
        # We use a filter which ignore the .git folder and the empty git trees
        super().__init__(*args, path_filter=list_git_tree, **kwargs)

    def fetch_artifact(self) -> Iterator[Path]:
        with raise_not_found_repository():
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_path = checkout_repository_ref(
                    self.origin.url,
                    self.git_ref,
                    target=Path(tmpdir),
                    max_tree_entries=self.max_tree_entries,
                    timeout=self.git_timeout,
                )
                if self.submodules:
                    local_clone = str(repo_path)
                    gitmodules_path = join(local_clone, ".gitmodules")
                    if exists(gitmodules_path):
                        with open(gitmodules_path, "r") as f:
                            gitmodules = f.read()
                        with open(gitmodules_path, "w") as f:
                            # replace no longer working github URLs using TCP protocol
                            f.write(
                                gitmodules.replace(
                                    "git://github.com/", "https://github.com/"
                                )
                            )
                        # --recursive checks out nested repositories that the
                        # entry count above does not cover; inside them only
                        # the timeout applies.
                        check_output(
                            [git(), "submodule", "update", "--init", "--recursive"],
                            cwd=local_clone,
                            timeout=self.git_timeout,
                        )
                        # restore the original .gitmodules in case it was
                        # modified above
                        check_output(
                            [git(), "checkout", "."],
                            cwd=local_clone,
                            timeout=self.git_timeout,
                        )
                yield repo_path

    def build_snapshot(self) -> Snapshot:
        """Build snapshot without losing the git reference context."""
        assert self.directory is not None
        branch_name = self.git_ref.encode()
        return Snapshot(
            branches={
                b"HEAD": SnapshotBranch(
                    target_type=SnapshotTargetType.ALIAS,
                    target=branch_name,
                ),
                branch_name: SnapshotBranch(
                    target=self.directory.id,
                    target_type=SnapshotTargetType.DIRECTORY,
                ),
            }
        )
