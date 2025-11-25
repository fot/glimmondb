import os
import sqlite3
from hashlib import sha256
from pathlib import Path

import pytest

TEST_DB_PATH = Path(os.environ.get("GLIMMON_TEST_DB", "testing_data/glimmondb.sqlite3"))

# Fingerprints captured from the current testing_data/glimmondb.sqlite3.
EXPECTED_FINGERPRINTS = {
    "limit_hash": "03e11c97619fb3068d1af730c12d41c01ff44ee5c9613186d6ed838b04be93df",
    "state_hash": "d24b4a66f89fedd8b59c123d32715eec31551962672cbc282b90c26379f41442",
    "version_hash": "8a746b74d3f66e43e5326fc34d01b36ccd50c488cf3a72451b214bb58b58de83",
    "limit_count": 4028,
    "state_count": 2242,
    "version_count": 422,
}


@pytest.fixture(scope="module")
def db_conn():
    if not TEST_DB_PATH.exists():
        pytest.skip(f"Test database not found at {TEST_DB_PATH}")
    conn = sqlite3.connect(TEST_DB_PATH)
    yield conn
    conn.close()


def _hash_rows(rows):
    return sha256(str(rows).encode("utf-8")).hexdigest()


def test_max_version_positive(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT MAX(version) FROM versions")
    version = cursor.fetchone()[0]
    assert version is not None and version > 0


def test_limits_by_msid(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM limits WHERE msid=?", ("tcylaft6",))
    rows = cursor.fetchall()
    assert rows, "Expected at least one row for msid tcylaft6"


def test_latest_limits_all_msids_unique(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        """SELECT a.msid, a.setkey, a.caution_high, a.caution_low, a.warning_high, a.warning_low
           FROM limits AS a
           WHERE a.modversion = (SELECT MAX(b.modversion) FROM limits AS b
                                 WHERE a.msid = b.msid AND a.setkey = b.setkey)"""
    )
    rows = cursor.fetchall()
    assert rows, "Expected latest limits rows"
    pairs = [(r[0], r[1]) for r in rows]
    assert len(pairs) == len(set(pairs)), "Duplicate msid/setkey pairs found"


def test_latest_limits_for_msid(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        """SELECT a.msid, a.setkey, a.mlmenable, a.caution_high, a.caution_low,
                  a.warning_high, a.warning_low
           FROM limits AS a
           WHERE a.msid=? AND a.modversion = (SELECT MAX(b.modversion) FROM limits AS b
                                              WHERE b.msid=a.msid)""",
        ("tephin",),
    )
    rows = cursor.fetchall()
    assert rows, "Expected rows for msid tephin"
    for row in rows:
        mlmenable = row[2]
        assert mlmenable in (0, 1), f"mlmenable should be 0 or 1, got {mlmenable}"


def test_latest_expected_states_all_msids_unique(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        """SELECT a.msid, a.setkey, a.expst
           FROM expected_states AS a
           WHERE a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                                 WHERE a.msid = b.msid AND a.setkey = b.setkey)"""
    )
    rows = cursor.fetchall()
    assert rows, "Expected latest expected_state rows"
    pairs = [(r[0], r[1]) for r in rows]
    assert len(pairs) == len(set(pairs)), "Duplicate msid/setkey pairs found"


def test_latest_expected_state_for_msid(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        """SELECT a.msid, a.setkey, a.mlmenable, a.expst
           FROM expected_states AS a
           WHERE a.msid=? AND a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                                              WHERE b.msid=a.msid)""",
        ("ebt2rly3",),
    )
    rows = cursor.fetchall()
    assert rows, "Expected rows for msid ebt2rly3"
    for row in rows:
        mlmenable = row[2]
        assert mlmenable in (0, 1), f"mlmenable should be 0 or 1, got {mlmenable}"


def test_fingerprints_match_expected(db_conn):
    cursor = db_conn.cursor()

    cursor.execute(
        """SELECT msid, setkey, datesec, date, modversion, mlmenable, mlmtol,
                  default_set, mlimsw, caution_high, caution_low, warning_high,
                  warning_low, switchstate
           FROM limits
           ORDER BY datesec, msid, setkey"""
    )
    limit_rows = cursor.fetchall()
    cursor.execute(
        """SELECT msid, setkey, datesec, date, modversion, mlmenable, mlmtol,
                  default_set, mlimsw, expst, switchstate
           FROM expected_states
           ORDER BY datesec, msid, setkey"""
    )
    state_rows = cursor.fetchall()
    cursor.execute(
        """SELECT version, datesec, date
           FROM versions
           ORDER BY datesec, version"""
    )
    version_rows = cursor.fetchall()

    assert len(limit_rows) == EXPECTED_FINGERPRINTS["limit_count"]
    assert len(state_rows) == EXPECTED_FINGERPRINTS["state_count"]
    assert len(version_rows) == EXPECTED_FINGERPRINTS["version_count"]

    assert _hash_rows(limit_rows) == EXPECTED_FINGERPRINTS["limit_hash"]
    assert _hash_rows(state_rows) == EXPECTED_FINGERPRINTS["state_hash"]
    assert _hash_rows(version_rows) == EXPECTED_FINGERPRINTS["version_hash"]
