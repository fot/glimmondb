#!/usr/bin/env python3
"""
Backup helper for G_LIMMON.dec files.

Given a source directory and destination directory, this script computes a SHA-256
fingerprint of the source G_LIMMON.dec file. If the hash differs from the last
recorded hash, it copies the file to the destination with the hash and UTC
timestamp appended to the filename.

Usage:
    python backup_glimmon.py --src /path/to/glimmon_archive --dst /path/to/backups

Optional args:
    --filename   Name of the file to watch (default: G_LIMMON.dec)
    --state-file Path to a state file to track last hash
                 (default: <dst>/.glimmon_backup_state.json)
"""

import argparse
import json
import shutil
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Dict


def compute_hash(file_path: Path) -> str:
    """
    Return the SHA-256 hex digest of the given file.

    Reads the file in 8KB chunks to avoid loading large files entirely into memory.
    """
    h = sha256()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_state(state_path: Path) -> Dict[str, str]:
    """
    Load previously recorded hashes from the state file.

    Returns an empty dict if the state file is missing or unreadable.
    """
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text())
    except Exception:
        return {}


def save_state(state_path: Path, state: Dict[str, str]) -> None:
    """
    Persist the hash state to disk as JSON.

    Ensures the parent directory exists before writing.
    """
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2))


def backup_if_changed(src_dir: Path, dst_dir: Path, filename: str, state_file: Path) -> None:
    """
    Compute the hash of the target file and back it up if contents have changed.

    Compares the current hash to the last stored hash; when different, copies the
    file to the destination with a timestamp and short-hash suffix, updates state,
    and prints a short summary. No copy occurs if the hash matches the stored value.
    """
    src_file = src_dir / filename
    if not src_file.exists():
        raise FileNotFoundError(f"Source file not found: {src_file}")

    dst_dir.mkdir(parents=True, exist_ok=True)

    state = load_state(state_file)
    current_hash = compute_hash(src_file)
    last_hash = state.get(str(src_file))

    if last_hash == current_hash:
        print(f"No change detected for {src_file}; last hash matches current hash.")
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short_hash = current_hash[:8]
    dest_name = f"{src_file.stem}_{timestamp}_{short_hash}{src_file.suffix}"
    dest_path = dst_dir / dest_name

    shutil.copy2(src_file, dest_path)
    state[str(src_file)] = current_hash
    save_state(state_file, state)

    print(f"Detected update. Copied {src_file} -> {dest_path}")
    print(f"Hash: {current_hash}")


def parse_args():
    """
    Parse CLI arguments for the backup script.

    Supports source/destination directories, filename override, and custom state file.
    """
    parser = argparse.ArgumentParser(description="Backup G_LIMMON.dec when contents change.")
    parser.add_argument("--src", required=True, help="Source directory containing G_LIMMON.dec")
    parser.add_argument("--dst", required=True, help="Destination directory for backups")
    parser.add_argument(
        "--filename", default="G_LIMMON.dec", help="Filename to monitor (default: G_LIMMON.dec)"
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="Path to state file (default: <dst>/.glimmon_backup_state.json)",
    )
    return parser.parse_args()


def main():
    """
    Entrypoint: parse arguments, resolve paths, and trigger conditional backup.
    """
    args = parse_args()
    src_dir = Path(args.src).expanduser()
    dst_dir = Path(args.dst).expanduser()
    state_file = (
        Path(args.state_file).expanduser()
        if args.state_file
        else dst_dir / ".glimmon_backup_state.json"
    )

    backup_if_changed(src_dir, dst_dir, args.filename, state_file)


if __name__ == "__main__":
    main()

# Additional Description:
#
# backup_glimmon.py flow:

#  - main():
#    - Calls parse_args() to read --src, --dst, optional --filename, optional --state-file.
#    - Resolves paths (src_dir, dst_dir, state_file; default state file is 
#      <dst>/.glimmon_backup_state.json).
#    - Invokes backup_if_changed(src_dir, dst_dir, filename, state_file).

#  - backup_if_changed(src_dir, dst_dir, filename, state_file):
#    - Builds src_file = src_dir/filename; errors if missing.
#    - Ensures dst_dir exists.
#    - Loads prior hashes from state_file via load_state().
#    - Computes current hash via compute_hash() (SHA-256 over 8KB chunks).
#    - Compares current_hash to last_hash for src_file:
#      - If unchanged: print “No change detected…” and return.
#      - If changed:
#        - Build timestamp (UTC, YYYYMMDD_HHMMSS) and short_hash (first 8 chars).
#        - Copy src_file to dst_dir as <stem><timestamp><short_hash><suffix> using shutil.copy2.
#        - Update state with the new hash and save it via save_state().
#        - Print a summary with source, destination, and full hash.
#
#  - compute_hash(file_path):
#    - Streams file contents into sha256() and returns the hex digest.
#
#  - load_state(state_path):
#    - Loads JSON from the state file; returns {} if missing/unreadable.
#
#  - save_state(state_path, state):
#    - Ensures parent directory exists, writes the state dict as JSON.
#
#  - parse_args():
#    - Defines the CLI interface and returns parsed arguments.
#
# Net effect: each run checks the watched G_LIMMON.dec (or custom filename) for content changes; 
# on change, it copies the file to the destination with a timestamp+short-hash suffix and records 
# the new hash to avoid redundant backups.
