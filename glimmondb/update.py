import errno
import glob
import json
import logging
import re
import shutil
import sqlite3
import sys
from hashlib import sha256
from logging import LoggerAdapter
from operator import itemgetter
from pathlib import Path
from os import environ
from os.path import join as pathjoin

from Chandra.Time import DateTime

from .glimmondb import (
    DBDIR,
    TDBDIR,
    _load_pickle_file,
    ensure_fingerprint_table,
    get_tdb,
    logfile,
    raise_tabletype_error,
    query_all_cols_one_row_to_copy,
    query_most_recent_changeable_data,
    query_most_recent_disabled_msids_sets,
    query_most_recent_msids_sets,
    read_glimmon,
    update_msid,
)


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for structured logs."""

    def format(self, record):
        data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        standard_keys = set(logging.LogRecord("logger", logging.INFO, "", 0, "", (), None).__dict__.keys())
        for key, value in record.__dict__.items():
            if key not in standard_keys and key not in ("message", "asctime"):
                data[key] = value
        return json.dumps(data, default=str)


def _configure_logging():
    """Attach file/stdout handlers to the package logger if none exist."""
    pkg_logger = logging.getLogger("glimmondb")
    stream_level_name = environ.get('GLIMMON_STREAM_LEVEL', 'INFO').upper()
    file_level_name = environ.get('GLIMMON_FILE_LEVEL', 'DEBUG').upper()
    stream_level = getattr(logging, stream_level_name, logging.INFO)
    file_level = getattr(logging, file_level_name, logging.DEBUG)
    pkg_logger.setLevel(min(stream_level, file_level))
    formatter = JsonFormatter()

    file_handler_present = any(
        isinstance(handler, logging.FileHandler) and Path(getattr(handler, "baseFilename", "")) == Path(logfile)
        for handler in pkg_logger.handlers
    )
    if not file_handler_present:
        file_handler = logging.FileHandler(logfile)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(file_level)
        pkg_logger.addHandler(file_handler)

    stream_handler_present = any(
        isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler)
        for handler in pkg_logger.handlers
    )
    if not stream_handler_present:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(stream_level)
        pkg_logger.addHandler(stream_handler)

    pkg_logger.propagate = False


def get_logger(name=None, **context):
    """Return a context-aware logger scoped to this package; configure handlers once."""
    _configure_logging()
    base_name = "glimmondb" if name is None else f"glimmondb.{name}"
    base_logger = logging.getLogger(base_name)
    return LoggerAdapter(base_logger, context)


logger = get_logger(__name__)


def _compute_hash(rows):
    return sha256(str(rows).encode('utf-8')).hexdigest()


def _compute_file_hash(filepath):
    h = sha256()
    path_obj = Path(filepath)
    with path_obj.open('rb') as fh:
        for chunk in iter(lambda: fh.read(8192), b''):
            h.update(chunk)
    return h.hexdigest(), path_obj.stat().st_size


def compute_fingerprints(db_path, source_file=None, source_revision=None, source_date=None):
    """Compute deterministic hashes/counts for key tables and the DB file."""
    db = sqlite3.connect(db_path)
    ensure_fingerprint_table(db)
    cursor = db.cursor()
    limit_query = """SELECT msid, setkey, datesec, date, modversion, mlmenable, mlmtol,
                     default_set, mlimsw, caution_high, caution_low, warning_high, warning_low,
                     switchstate FROM limits ORDER BY datesec, msid, setkey"""
    state_query = """SELECT msid, setkey, datesec, date, modversion, mlmenable, mlmtol,
                     default_set, mlimsw, expst, switchstate FROM expected_states
                     ORDER BY datesec, msid, setkey"""
    version_query = """SELECT version, datesec, date FROM versions ORDER BY datesec, version"""

    cursor.execute(limit_query)
    limit_rows = cursor.fetchall()
    limit_hash = _compute_hash(limit_rows)

    cursor.execute(state_query)
    state_rows = cursor.fetchall()
    state_hash = _compute_hash(state_rows)

    cursor.execute(version_query)
    version_rows = cursor.fetchall()
    version_hash = _compute_hash(version_rows)

    cursor.execute("""SELECT MAX(version) FROM versions""")
    current_version = cursor.fetchone()[0]

    db_sha, db_size = _compute_file_hash(db_path)

    cursor.execute(
        """INSERT OR REPLACE INTO build_fingerprints(
           version, limit_hash, state_hash, version_hash,
           limit_count, state_count, version_count,
           db_sha256, db_size_bytes,
           source_file, source_revision, source_date
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (current_version, limit_hash, state_hash, version_hash,
         len(limit_rows), len(state_rows), len(version_rows),
         db_sha, db_size, source_file, source_revision, str(source_date))
    )
    db.commit()

    fingerprint_logger = get_logger(__name__, tabletype='fingerprint', revision=current_version)
    fingerprint_logger.info('Database fingerprint computed',
                            extra={'limit_hash': limit_hash,
                                   'state_hash': state_hash,
                                   'version_hash': version_hash,
                                   'limit_count': len(limit_rows),
                                   'state_count': len(state_rows),
                                   'version_count': len(version_rows),
                                   'db_sha256': db_sha,
                                   'db_size_bytes': db_size,
                                   'source_file': source_file,
                                   'source_revision': source_revision,
                                   'source_date': source_date})

    db.close()


class GLimit(object):

    """ G_LIMMON instance for outputting es/limit data in row format.

    This is used to convert the dictionary of es/limit data for each msid and set into
    line entries, where each line contains all the required information for each msid and set
    pair. This resulting format is how the sqlite3 database tables for limits and expected
    states (separate tables) are structured.

    Remember that an MSID/Set pair defines a unique condition, for one point in time.

    """

    def __init__(self, gdb, discard_disabled_sets=True):
        """ Create row-based tables for limits and expected states from an imported G_LIMMON file.

        :param gdb: Dictionary containing the definitions defined in one G_LIMMON file.

        Note: the input argument "gdb" is expected to have TDB data filled in where limits and
        expected states are not explicitly defined.
        """
        self.gdb = gdb
        self.rootfields = ['mlmenable', 'mlmtol', 'default', 'mlimsw']
        self.limitsetfields = [
            'caution_high', 'caution_low', 'warning_high', 'warning_low', 'switchstate']
        self.statesetfields = ['expst', 'switchstate']
        self.msids = self.filter_msids()
        d = self.date
        self.date = '{}-{}-{} {}:{}:{}'.format(d[0], d[1], d[2], d[3], d[4], d[5])
        self.datesec = DateTime(self.date).secs
        self.discard_disabled_sets = discard_disabled_sets

    def filter_msids(self):
        """ Assign an "msids" attribute to this class.

        This list of msids is generated from the self.gdb dictionary keys with non-msid keys
        removed.
        """
        msids = list(self.gdb.keys())
        removekeys = ['revision', 'version', 'mlmdeftol', 'mlmthrow', 'date']
        for key in removekeys:
            self.__setattr__(key, self.gdb[key])
            msids.remove(key)

        # MSIDs that have no type will not have any defined limits/expected states (e.g. coscs098s)
        for msid in msids:
            if 'type' not in list(self.gdb[msid].keys()):
                msids.remove(msid)
        return msids

    def gen_row_data(self):
        """ Return G_LIMMON limit and expected state definitions in row format.

        The returned list will be structured to be compatible with the final sqlite3 limit
        and expected state definition tables.
        """
        limitrowdata = []
        esstaterowdata = []

        for msid in self.msids:
            mdata = self.gdb[msid]

            # If an msid is disabled in the TDB and in G_LIMMON, it may not have a 'limit' or
            # 'expst' designation. Work around this by looking for 'mlmenable':0.
            if ('mlmenable', 0) not in mdata.items():

                if 'type' not in mdata.keys():
                    print('{} is not properly defined and will be removed'.format(msid))

                    textinsert1 = '     WARNING: {} has no limits or expected states '.format(msid)
                    textinsert2 = 'defined in the database or G_LIMMON, yet is enabled in G_LIMMON'
                    msid_logger = get_logger(__name__, msid=msid.lower(),
                                             revision=self.revision, date=self.date)
                    msid_logger.warning(textinsert1 + textinsert2)
                    msid_logger.warning('{} will be removed'.format(msid))

                elif mdata['type'].lower() == 'limit':
                    for setkey in mdata['setkeys']:
                        if setkey in list(mdata.keys()):
                            rowdata = []
                            rowdata.append(msid.lower())
                            rowdata.append(setkey)
                            rowdata.append(self.datesec)
                            rowdata.append(self.date)
                            # only want values after decimal point
                            rowdata.append(int(self.revision[2:]))
                            # rowdata.append(1) # Active

                            if 'mlmenable' in list(mdata.keys()):
                                rowdata.append(mdata['mlmenable'])
                            else:
                                rowdata.append('1')

                            if 'mlmtol' in list(mdata.keys()):
                                rowdata.append(mdata['mlmtol'])
                            else:
                                rowdata.append('1')

                            if 'default' in list(mdata.keys()):
                                rowdata.append(mdata['default'])
                            else:
                                rowdata.append('0')

                            if 'mlimsw' in list(mdata.keys()):
                                rowdata.append(mdata['mlimsw'].lower())
                            else:
                                rowdata.append('none')

                            if 'caution_high' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['caution_high'])
                            else:
                                rowdata.append('none')

                            if 'caution_low' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['caution_low'])
                            else:
                                rowdata.append('none')

                            if 'warning_high' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['warning_high'])
                            else:
                                rowdata.append('none')

                            if 'warning_low' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['warning_low'])
                            else:
                                rowdata.append('none')

                            if 'switchstate' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['switchstate'].lower())
                            else:
                                rowdata.append('none')

                            limitrowdata.append(rowdata)

                elif mdata['type'].lower() == 'expected_state':
                    for setkey in mdata['setkeys']:
                        if setkey in list(mdata.keys()):
                            rowdata = []
                            rowdata.append(msid.lower())
                            rowdata.append(setkey)
                            rowdata.append(self.datesec)
                            rowdata.append(self.date)
                            # only want values after decimal point
                            rowdata.append(int(self.revision[2:]))
                            # rowdata.append(1) # Active

                            if 'mlmenable' in list(mdata.keys()):
                                rowdata.append(mdata['mlmenable'])
                            else:
                                rowdata.append('1')

                            if 'mlmtol' in list(mdata.keys()):
                                rowdata.append(mdata['mlmtol'])
                            else:
                                rowdata.append('1')

                            if 'default' in list(mdata.keys()):
                                rowdata.append(mdata['default'])
                            else:
                                rowdata.append('0')

                            if 'mlimsw' in list(mdata.keys()):
                                rowdata.append(mdata['mlimsw'].lower())
                            else:
                                rowdata.append('none')

                            if 'expst' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['expst'].lower())
                            else:
                                rowdata.append('none')

                            if 'switchstate' in list(mdata[setkey].keys()):
                                rowdata.append(mdata[setkey]['switchstate'].lower())
                            else:
                                rowdata.append('none')

                            esstaterowdata.append(rowdata)

        # Remove disabled MSID rows, this will be taken care of later when these
        # are found to be missing
        if self.discard_disabled_sets:
            for n, row in enumerate(limitrowdata):
                if int(row[5]) == 0:
                    _ = limitrowdata.pop(n)

        # Remove disabled MSID rows, this will be marked as disabled later when
        # these are found to be missing
        if self.discard_disabled_sets:
            for n, row in enumerate(esstaterowdata):
                if int(row[5]) == 0:
                    _ = esstaterowdata.pop(n)

        return limitrowdata, esstaterowdata


    def write_limit_row_data(self, limitrowdata):
        """ Write limit table to file.

        This is used for debugging purposes.
        """
        fid = open('limitrowdata_{}.txt'.format(self.revision[2:]), 'w')
        fid.writelines([','.join([str(s) for s in row]) + '\n' for row in limitrowdata])
        fid.close()

    def write_state_row_data(self, esstaterowdata):
        """ Write expected state table to file.

        This is used for debugging purposes.
        """
        fid = open('esstaterowdata_{}.txt'.format(self.revision[2:]), 'w')
        fid.writelines([','.join([str(s) for s in row]) + '\n' for row in esstaterowdata])
        fid.close()


class NewLimitDB(object):

    """ Create a G_LIMMON based sqlite3 database.

    This writes the G_LIMMON data formatted using the GLimit class to an sqlite3 database file.

    """

    def __init__(self, limitrowdata, esstaterowdata, version, date, datesec):
        """ Create Sqlite3 database for limits, expected states, and version information.

        :param limitrowdata: List of limit definitions, each row defines limits for one msid/set
        :param esstaterowdata: List of state definitions, each row defines states for one msid/set
        :param version: GLIMMON version for data in limitrowdata and esstaterowdata
        :param date: Date in HOSC format corresponding to the version number
        :param datesec: Date in seconds from '1997:365:23:58:56.816' format
        """
        temp_filename = pathjoin(DBDIR, 'temporary_db.sqlite3')
        try:
            shutil.rmtree(temp_filename)
        except OSError:
            pass
        self.db = sqlite3.connect(temp_filename)
        self.create_limit_table()
        self.fill_limit_data(limitrowdata)
        self.create_esstate_table()
        self.fill_esstate_data(esstaterowdata)
        self.create_version_table()
        self.fill_version_data(version, date, datesec)
        ensure_fingerprint_table(self.db)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.db.close()

    def create_limit_table(self):
        cursor = self.db.cursor()
        cursor.execute("""DROP TABLE IF EXISTS limits""")
        cursor.execute("""CREATE TABLE limits(id INTEGER PRIMARY KEY, msid TEXT, setkey INTEGER, datesec REAL,
                          date TEXT, modversion INTEGER, mlmenable INTEGER, mlmtol INTEGER,
                          default_set INTEGER, mlimsw TEXT, caution_high REAL, caution_low REAL, warning_high REAL,
                          warning_low REAL, switchstate TEXT)""")
        self.db.commit()

    def create_esstate_table(self):
        cursor = self.db.cursor()
        cursor.execute("""DROP TABLE IF EXISTS expected_states""")
        cursor.execute("""CREATE TABLE expected_states(id INTEGER PRIMARY KEY, msid TEXT, setkey INTEGER,
                          datesec REAL, date TEXT, modversion INTEGER, mlmenable INTEGER,
                          mlmtol INTEGER, default_set INTEGER, mlimsw TEXT, expst TEXT, switchstate TEXT)""")
        self.db.commit()

    def fill_limit_data(self, limitrowdata):
        cursor = self.db.cursor()
        for row in limitrowdata:
            cursor.execute("""INSERT INTO limits(msid, setkey, datesec, date, modversion, mlmenable, mlmtol,
                              default_set, mlimsw, caution_high, caution_low, warning_high, warning_low,
                              switchstate) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
        self.db.commit()

    def fill_esstate_data(self, esstaterowdata):
        cursor = self.db.cursor()
        for row in esstaterowdata:
            cursor.execute("""INSERT INTO expected_states(msid, setkey, datesec, date, modversion, mlmenable,
                              mlmtol, default_set, mlimsw, expst, switchstate) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", row)
        self.db.commit()

    def create_version_table(self):
        cursor = self.db.cursor()
        cursor.execute("""DROP TABLE IF EXISTS versions""")
        cursor.execute(
            """CREATE TABLE versions(id INTEGER PRIMARY KEY, version INTEGER UNIQUE, datesec REAL UNIQUE, date TEXT UNIQUE)""")
        self.db.commit()

    def fill_version_data(self, version, date, datesec):
        cursor = self.db.cursor()
        cursor.execute(
            """INSERT INTO versions(version, datesec, date) VALUES(?,?,?)""", (version, datesec, date))
        self.db.commit()


def commit_new_rows(db, rows, tabletype):
    """ Commit new limit table or expected state table rows to an sqlite3 database.

    :param db: sqlite3 database connection
    :param rows: list of rows to be added to an sqlite3 table
    :param tabletype: 'limit' or 'expected_state'

    """
    oldcursor = db.cursor()
    if tabletype.lower() == 'limit':
        for row in rows:
            oldcursor.execute("""INSERT INTO limits(msid, setkey, datesec, date, modversion, mlmenable, mlmtol,
                                 default_set, mlimsw, caution_high, caution_low, warning_high, warning_low, switchstate)
                                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
            row_logger = get_logger(__name__, msid=row[0], setkey=row[1], datesec=row[2],
                                    revision=row[4], tabletype=tabletype)
            row_logger.info('Added limit row')

    elif tabletype.lower() == 'expected_state':
        for row in rows:
            oldcursor.execute("""INSERT INTO expected_states(msid, setkey, datesec, date, modversion, mlmenable,
                                 mlmtol, default_set, mlimsw, expst, switchstate) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", row)
            row_logger = get_logger(__name__, msid=row[0], setkey=row[1], datesec=row[2],
                                    revision=row[4], tabletype=tabletype)
            row_logger.info('Added expected_state row')
    else:
        raise_tabletype_error(tabletype)

    db.commit()


def commit_new_version_row(olddb, newdb):
    """ Commit new version table row to an sqlite3 database.

    :param olddb: sqlite3 database connection for old database (current database to be updated)
    :param newdb: sqlite3 database connection for new database (sqlite version of newest G_LIMMON)

    This is similar to "commit_new_rows()" but updates only the "version" table to keep track of
    the history of G_LIMMON versions added to the database.
    """

    newcursor = newdb.cursor()
    data = newcursor.execute("""SELECT version, datesec, date FROM versions""")
    version, datesec, date = data.fetchone()

    print('Imported: Version: {}, datesec: {}, date: {}'.format(version, datesec, date))

    oldcursor = olddb.cursor()
    oldcursor.execute(
        """INSERT INTO versions(version, datesec, date) VALUES(?,?,?)""", (version, datesec, date))
    version_logger = get_logger(__name__, tabletype='versions', revision=version, datesec=datesec,
                                version_date=date)
    version_logger.info('Committed version row')
    olddb.commit()


def add_merge_logging_text(tabletype, functiontype, newlen, oldlen, addlen):
    """ Add Merge Operation Logging.

    :param tabletype: either 'limit' or 'expected_state'
    :param functiontype: either 'added', 'deactivated', or 'modified'

    """

    formatted_tabletype = ' '.join([s.capitalize() for s in tabletype.split('_')])
    formatted_functiontype = functiontype.capitalize()
    merge_logger = get_logger(__name__, tabletype=tabletype)
    merge_logger.info('Comparing new and current %s table for %s msid sets',
                      formatted_tabletype, formatted_functiontype)
    merge_logger.debug('Row counts new=%s current=%s', newlen, oldlen)
    merge_logger.info('Rows to be %s for newly added msid sets: %s',
                      formatted_functiontype.lower(), addlen)
    merge_logger.info('Appending rows for %s msid sets', formatted_tabletype)


def merge_added_msidsets(newdb, olddb, tabletype):
    """ Merge new/modified data to the current database.

    :param olddb: sqlite3 database connection for old database (current database to be updated)
    :param newdb: sqlite3 database connection for new database (sqlite version of newest G_LIMMON)
    :param tabletype: type of table ('limit' or 'expected_state')

    """

    all_new_rows = query_most_recent_msids_sets(newdb, tabletype)
    all_old_rows = query_most_recent_msids_sets(olddb, tabletype)

    # what is in newrows but not in oldrows
    not_in_oldrows = sorted(
        set(all_new_rows).difference(set(all_old_rows)), key=lambda row: (row[0], row[1]))
    addrows = []
    for msidset in not_in_oldrows:
        addrow = query_all_cols_one_row_to_copy(newdb, msidset, tabletype)
        addrows.append(addrow)

    add_merge_logging_text(tabletype, 'added', len(all_new_rows), len(all_old_rows), len(addrows))
    commit_new_rows(olddb, addrows, tabletype)


def merge_deleted_msidsets(newdb, olddb, tabletype):
    """ Find newly deleted/disabled msids and add new rows indicating the change in status to db.

    :param newdb: Database object for new G_LIMMON
    :param olddb: Database object for current database
    :param tabletype: type of table ('limit' or 'expected_state')

    All disabled msids are removed from the new G_LIMMON when read in. This means that there needs
    to be several steps to determine which msids/sets are newly deleted.
    """

    newcursor = newdb.cursor()
    data = newcursor.execute("""SELECT version, datesec, date FROM versions""")
    version, datesec, date = data.fetchone()

    all_new_rows = query_most_recent_msids_sets(newdb, tabletype)
    all_old_rows = query_most_recent_msids_sets(olddb, tabletype)
    all_old_disabled_rows = query_most_recent_disabled_msids_sets(olddb, tabletype)

    disabled_not_in_newrows = sorted(
        set(all_old_disabled_rows).difference(set(all_new_rows)), key=lambda row: (row[0], row[1]))
    all_new_rows.extend(list(disabled_not_in_newrows))

    # what is in oldrows but not in newrows
    not_in_newrows = sorted(
        set(all_old_rows).difference(set(all_new_rows)), key=lambda row: (row[0], row[1]))
    deactivaterows = []
    for msidset in not_in_newrows:
        deactrow = query_all_cols_one_row_to_copy(olddb, msidset, tabletype)
        deactrow = list(deactrow)
        deactrow[2] = datesec
        deactrow[3] = date
        deactrow[4] = version
        deactrow[5] = 0
        deactivaterows.append(tuple(deactrow))

    add_merge_logging_text(tabletype, 'deactivated', len(
        all_new_rows), len(all_old_rows), len(deactivaterows))
    commit_new_rows(olddb, deactivaterows, tabletype)


def merge_modified_msidsets(newdb, olddb, tabletype):
    """ Find newly modified msids and add new rows indicating the change in status/info to db.

    :param newdb: Database object for new G_LIMMON
    :param olddb: Database object for current database
    :param tabletype: type of table ('limit' or 'expected_state')

    """
    all_new_rows = query_most_recent_changeable_data(newdb, tabletype)
    all_old_rows = query_most_recent_changeable_data(olddb, tabletype)

    # what is in newrows but not in oldrows
    modified_rows = sorted(
        set(all_new_rows).difference(set(all_old_rows)), key=lambda row: (row[0], row[1]))
    modrows = []
    newcursor = newdb.cursor()
    for msidset in modified_rows:
        modrow = query_all_cols_one_row_to_copy(newdb, msidset[:2], tabletype)
        modrows.append(modrow)

    add_merge_logging_text(
        tabletype, 'modified', len(all_new_rows), len(all_old_rows), len(modrows))
    commit_new_rows(olddb, modrows, tabletype)


def create_db(gdb, discard_disabled_sets=True):
    """ Create new sqlite3 database based on a G_LIMMON definition.

    :param gdb: Dictionary containing a G_LIMMON definition

    """
    g = GLimit(gdb, discard_disabled_sets)
    # esstaterowdata2 = g.gen_state_row_data()
    # limitrowdata2 = g.gen_limit_row_data()
    limitrowdata2, esstaterowdata2 = g.gen_row_data()

    version = int(str(gdb['revision'])[2:])
    newdb_obj = NewLimitDB(limitrowdata2, esstaterowdata2, version, g.date, g.datesec)
    return newdb_obj.db


def recreate_db(glimmondbfile='glimmondb.sqlite3'):
    """ Recreate the G_LIMMON history sqlite3 database from all G_LIMMON.dec past versions.

    :param glimmondbfile: Filename for glimmon database, defaults to 'glimmondb.sqlite3'

    """

    def write_initial_db(gdb, glimmondbfile):
        """ Write Initial DB to Disk
        """

        def copy_anything(src, dst):
            """ Copied from Stackoverflow

            http://stackoverflow.com/questions/1994488/copy-file-or-directory-in-python
            """
            try:
                shutil.copytree(src, dst)
            except OSError as exc:  # python >2.5
                if exc.errno == errno.ENOTDIR:
                    shutil.copy(src, dst)
                else:
                    raise

        newdb = create_db(gdb, discard_disabled_sets=True)
        newdb.close()
        temp_filename = pathjoin(DBDIR, 'temporary_db.sqlite3')
        db_filename = pathjoin(DBDIR, glimmondbfile)
        copy_anything(temp_filename, db_filename)

        init_logger = get_logger(__name__, tabletype='initialization',
                                 db_filename=glimmondbfile)
        init_logger.info('Initialized %s', glimmondbfile)
        compute_fingerprints(db_filename, source_file="G_LIMMON_P007A.dec",
                             source_revision=gdb.get('revision'),
                             source_date=gdb.get('date'))

    def get_glimmon_arch_filenames():
        glimmon_files = glob.glob(pathjoin(DBDIR, 'G_LIMMON_2.*.dec'))
        return glimmon_files

    def get_glimmon_versions(glimmon_files):

        filename_rev_pattern = "G_LIMMON_([0-9]+).([0-9]+).dec"
        versions = []
        for filename in glimmon_files:
            rev = re.findall(filename_rev_pattern, filename)[0]
            versions.append([int(n) for n in rev])

        return sorted(versions, key=itemgetter(0, 1))

    filename = pathjoin(DBDIR, "G_LIMMON_P007A.dec")
    g = read_glimmon(filename)
    g['revision'] = '2.0'

    tdbfile = pathjoin(TDBDIR, 'tdb_all.pkl')
    tdbs = _load_pickle_file(tdbfile)
    tdb = get_tdb(tdbs, g['revision'][2:])

    for msid in list(g.keys()):
        update_msid(msid.lower(), tdb, g)

    write_initial_db(g, glimmondbfile)

    glimmon_files = get_glimmon_arch_filenames()
    revisions = get_glimmon_versions(glimmon_files)

    for rev in revisions:
        print(("Importing revision {}-{}".format(rev[0], rev[1])))
        gfile = pathjoin(DBDIR, "G_LIMMON_{}.{}.dec".format(rev[0], rev[1]))
        merge_new_glimmon_to_db(gfile, tdbs, glimmondbfile)


def merge_new_glimmon_to_db(filename, tdbs, glimmondbfile='glimmondb.sqlite3'):
    """ Merge a new G_LIMMON.dec file into the sqlite3 database.

    :param filename: Full path + filename for new G_LIMMON.dec file
    :param tdbs: Dictionary containing data from all TDB versions
    :param glimmondbfile: Filename for glimmon database, defaults to 'glimmondb.sqlite3'

    """

    g = read_glimmon(filename)

    tdb = get_tdb(tdbs, g['revision'][2:])
    for msid in list(g.keys()):
        update_msid(msid.lower(), tdb, g)

    newver = g['revision'][2:]

    glimmondb_filename = pathjoin(DBDIR, glimmondbfile)
    olddb = sqlite3.connect(glimmondb_filename)
    oldcursor = olddb.cursor()
    oldcursor.execute("""SELECT MAX(version) FROM versions""")
    oldver = oldcursor.fetchone()[0]

    textinsert = 'Comparing New (v{}) and Current (v{}) G_LIMMON Databases'.format(newver, oldver)
    import_logger = get_logger(__name__, revision=newver, tabletype='version',
                               glimmon_filename=filename)
    import_logger.info('Imported G_LIMMON file %s with revision %s and date %s',
                       filename, g.get('revision'), g.get('date'))
    import_logger.info(textinsert)

    newdb = create_db(g)

    # Limit Table Comparison/Merge
    merge_added_msidsets(newdb, olddb, 'limit')
    merge_deleted_msidsets(newdb, olddb, 'limit')
    merge_modified_msidsets(newdb, olddb, 'limit')

    # Expected State Table Comparison/Merge
    merge_added_msidsets(newdb, olddb, 'expected_state')
    merge_deleted_msidsets(newdb, olddb, 'expected_state')
    merge_modified_msidsets(newdb, olddb, 'expected_state')

    commit_new_version_row(olddb, newdb)
    compute_fingerprints(glimmondb_filename, source_file=filename,
                         source_revision=g.get('revision'), source_date=g.get('date'))

    newdb.close()
    olddb.close()

    temp_filename = pathjoin(DBDIR, 'temporary_db.sqlite3')
    try:
        shutil.rmtree(temp_filename)
    except OSError:
        pass
