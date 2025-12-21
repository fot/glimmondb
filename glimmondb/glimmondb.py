
import pickle as pickle
from pathlib import Path
from os import environ
from os.path import join as pathjoin
import numpy as np
import re
import logging

def _require_path(primary_var, fallback_var=None, fallback_suffix=None):
    """Return a Path from env vars and fail fast when missing or nonexistent."""
    if environ.get(primary_var):
        base_path = Path(environ[primary_var])
    elif fallback_var and environ.get(fallback_var):
        base_path = Path(environ[fallback_var])
        if fallback_suffix:
            base_path = base_path / fallback_suffix
    else:
        vars_list = [primary_var]
        if fallback_var:
            vars_list.append(fallback_var)
        raise RuntimeError(f"Missing required environment variable(s): {', '.join(vars_list)}")

    if not base_path.exists():
        raise FileNotFoundError(f"Configured path does not exist: {base_path}")
    return base_path


DBDIR = _require_path('GLIMMONDATA', 'SKA_DATA', 'glimmon_archive')
TDBDIR = _require_path('TDBDATA', 'SKA_DATA', 'fot_tdb_archive')

logfile = str((Path(DBDIR) / 'DB_Commit.log').resolve())

logging.getLogger("glimmondb").addHandler(logging.NullHandler())

def _load_pickle_file(filepath):
    """Load a pickle file from disk with a minimal existence check."""
    file_path = Path(filepath)
    if not file_path.exists():
        raise FileNotFoundError(f"Pickle file not found: {file_path}")
    with file_path.open('rb') as fh:
        return pickle.load(fh)

def ensure_fingerprint_table(db):
    cursor = db.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS build_fingerprints(
           id INTEGER PRIMARY KEY,
           version INTEGER UNIQUE,
           limit_hash TEXT,
            state_hash TEXT,
            version_hash TEXT,
            limit_count INTEGER,
            state_count INTEGER,
            version_count INTEGER,
            db_sha256 TEXT,
            db_size_bytes INTEGER,
            source_file TEXT,
            source_revision TEXT,
            source_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    db.commit()

def get_tdb(tdbs=None, revision=000, return_dates=False):
    """ Retrieve appropriate telemetry database

    :param tdb: tdb dictionary object
    :param revision: associated G_LIMMON dec file revision number
    :param return_dates: Flag to only return start dates for all databases.

    :returns: Dictionary containing the relevant data from the appropriate TDB

    This needs to be updated each time a new TDB version is introduced. Due to this level of
    maintenance, I've merged the actions to either return the tdb or return the start dates
    for all databases since both the dates and new revision numbers need to be updated.

    """

    startdates = {'p007': '1999:203:00:00:00', 'p009': '2008:024:21:00:00',
                  'p010': '2012:089:20:00:00', 'p011': '2014:156:20:00:00',
                  'p012': '2014:338:21:00:00', 'p013': '2015:162:20:00:00',
                  'p014': '2015:239:20:00:00', 'p015': '2021:063:21:00:00',
                  'p016': '2021:343:21:00:00', 'p017': '2024:151:20:00:00'}

    if return_dates:
        return startdates

    else:
        if not tdbs:
            tdbfile = pathjoin(TDBDIR, "tdb_all.pkl")
            tdbs = _load_pickle_file(tdbfile)

        revision = int(revision)
        if revision <= 130:
            print ('Using P007')
            tdb = tdbs['p007']
        elif revision <= 228:  # p009 starts with 131 and ends with 228 (inclusive)
            print ('Using P009')
            tdb = tdbs['p009']
        elif revision <= 245:  # p010 starts with 229 and ends with 246 (inclusive)
            print ('Using P010')
            tdb = tdbs['p010']
        elif revision <= 249:  # p011 starts with 246 and ends with 249 (inclusive)
            print ('Using P011')
            tdb = tdbs['p011']
        elif revision <= 256:  # p012 starts with 250 and ends with 256 (inclusive)
            print ('Using P012')
            tdb = tdbs['p012']
        elif revision <= 260:  # p013 starts with 257 and ends with 260
            print ('Using P013')
            tdb = tdbs['p013']
        elif revision <= 342:  # p014 starts with 261 and ends with 342
            print ('Using P014')
            tdb = tdbs['p014']
        elif revision <= 359:  # p015 starts with 343 and ends with 359
            print ('Using P015')
            tdb = tdbs['p015']
        elif revision <= 407:  # p016 starts with 360 and ends with 407
            print ('Using P016')
            tdb = tdbs['p016']
        elif revision <= 999:  # p017 starts with 408 and ends with ?
            print ('Using P017')
            tdb = tdbs['p017']
        return tdb


def read_glimmon(filename='/home/greta/AXAFSHARE/dec/G_LIMMON.dec'):
    """ Import G_LIMMON.dec file

    :param filename: Name/Location of G_LIMMON file to import

    :returns: Dictionary containing the import G_LIMMON information

    """

    revision_pattern = r'\s*#\s*\$Revision\s*:\s*([0-9.]+).*$'
    date_pattern = r'.*\$Date\s*:\s*([0-9]+)/([0-9]+)/([0-9]+)\s+([0-9]+):([0-9]+):([0-9]+).*$'

    # Read the GLIMMON.dec file and store each line in "gfile"
    with open(filename, 'r') as fid:
        gfile = fid.readlines()

    # Initialize the glimmon dictionary
    glimmon = {}

    # Step through each line in the GLIMMON.dec file
    for line in gfile:

        comment_line = line[line.find('#'):].strip()

        # Remove comments
        line = line[:line.find('#')].strip()

        # Assume the line uses whitespace as a delimiter
        words = line.split()

        if words:
            # Only process lines that begin with MLOAD, MLIMIT, MLMTOL, MLIMSW, MLMENABLE,
            # MLMDEFTOL, or MLMTHROW. This means that all lines with equations are
            # omitted; we are only interested in the limits and expected states

            if (words[0] == 'MLOAD') & (len(words) == 2):
                name = words[1]
                glimmon.update({name: {}})

            elif (words[0] == 'MMSID'):
                name = words[1]
                glimmon.update({name: {}})

            elif words[0] == 'MLIMIT':
                setnum = int(words[2])
                glimmon[name].update({setnum: {}})
                if 'setkeys' in glimmon[name]:
                    glimmon[name]['setkeys'].append(setnum)
                else:
                    glimmon[name]['setkeys'] = [setnum, ]

                if 'DEFAULT' in words:
                    glimmon[name].update({'default': setnum})

                if 'SWITCHSTATE' in words:
                    ind = words.index('SWITCHSTATE')
                    glimmon[name][setnum].update({'switchstate': words[ind + 1]})

                if 'PPENG' in words:
                    ind = words.index('PPENG')
                    glimmon[name].update({'type': 'limit'})
                    glimmon[name][setnum].update({'warning_low':
                                                  float(words[ind + 1])})
                    glimmon[name][setnum].update({'caution_low':
                                                  float(words[ind + 2])})
                    glimmon[name][setnum].update({'caution_high':
                                                  float(words[ind + 3])})
                    glimmon[name][setnum].update({'warning_high':
                                                  float(words[ind + 4])})

                if 'EXPST' in words:
                    ind = words.index('EXPST')
                    glimmon[name].update({'type': 'expected_state'})
                    glimmon[name][setnum].update({'expst': words[ind + 1]})

            elif words[0] == 'MLMTOL':
                glimmon[name].update({'mlmtol': int(words[1])})

            elif words[0] == 'MLIMSW':
                glimmon[name].update({'mlimsw': words[1]})

            elif words[0] == 'MLMENABLE':
                glimmon[name].update({'mlmenable': int(words[1])})

            elif words[0] == 'MLMDEFTOL':
                glimmon.update({'mlmdeftol': int(words[1])})

            elif words[0] == 'MLMTHROW':
                glimmon.update({'mlmthrow': int(words[1])})

            elif len(re.findall(revision_pattern, line)) > 0:
                revision = re.findall(revision_pattern, line)
                glimmon.update({'revision': revision[0].strip()})
                glimmon.update({'version': revision[0].strip()})

        elif len(re.findall(revision_pattern, comment_line)) > 0:
            revision = re.findall(revision_pattern, comment_line)
            glimmon.update({'revision': revision[0].strip()})
            glimmon.update({'version': revision[0].strip()})

        elif len(re.findall(date_pattern, comment_line)) > 0:
            date = re.findall(date_pattern, comment_line)
            glimmon.update({'date': date[0]})

    return glimmon


def assign_sets(dbsets):
    """ Copy over only the limit/expst sets, other stuff is not copied.

    :param dbsets: Datastructure stored in the TDB as tdb[msid]['limit']

    :returns: Dictionary containing the relevant limit/expected state data

    This also adds a list of set numbers using zero-based numbering.
    """
    limits = {'setkeys': []}
    for setnum in list(dbsets.keys()):
        setnumint = int(setnum) - 1
        limits.update({setnumint: dbsets[setnum]})
        limits['setkeys'].append(setnumint)
    return limits


def is_not_nan(arg):
    """ Test to see if a variable is not a nan type.

    :param arg: Variable to be tested

    :returns: True if a variable is not a nan type, otherwise False

    The Numpy isnan function only works on numeric data (including arrays), not strings or other
    types. This "fixes" this function so that it returns a False if the argument is a nan type,
    regardless of input type. This function returns a True if it is anything but a nan type.
    """
    try:
        np.isnan(arg)
    except (TypeError, NotImplementedError):
        return True
    return False


def fill_limits(tdb, g, msid):
    """ Fill in tdb limit data where none are explicitly specified in G_LIMMON.

    :param tdb: TDB datastructure (corresponding to p012, p013, etc.)
    :param g: G_LIMMON datastructure (corresponding to a single version, e.g. 2.256)
    :param msid: Current MSID

    There is no return value, the "g" datastructure is updated in place.
    """

    limits = assign_sets(tdb[msid]['limit'])

    limits['type'] = 'limit'

    if is_not_nan(tdb[msid]['limit_default_set_num']):
        limits['default'] = tdb[msid]['limit_default_set_num'] - 1
    else:
        limits['default'] = 0

    # Alternate limit/es sets are automatically added to GLIMMON by GRETA, GLIMMON only adds MSIDs
    # to the current set of MSIDs and *prepends* limit/es sets that will take precedence
    # over sets defined in the database.
    if is_not_nan(tdb[msid]['limit_switch_msid']):
        limits['mlimsw'] = tdb[msid]['limit_switch_msid']

        if 'lim_switch' in list(tdb[msid].keys()):
            for setkey in limits['setkeys']:  # assuming there are limit switch states for each set
                indkey = setkey + 1
                try:
                    limits[setkey]['switchstate'] = tdb[msid]['lim_switch'][indkey]['state_code']
                except:
                    limits[setkey]['switchstate'] = 'none'

    # for setkey in limits['setkeys']:
    #     if 'state_code' in limits[setkey].keys():
    #         limits[setkey]['switchstate'] = limits[setkey]['state_code']
    #         _ = limits[setkey].pop('state_code')

    # Fill in the default tolerance specified in the GLIMMON file
    if 'mlmtol' not in list(g[msid.upper()].keys()):
        limits['mlmtol'] = g['mlmdeftol']

    # if mlmenable is not specified, set it to 1 (enabled) as a default
    if 'mlmenable' not in list(g[msid.upper()].keys()):
        limits['mlmenable'] = 1

    # GLIMMON values take precedence over database values so if any are defined, update
    # the database dict with the GLIMMON fields
    limits.update(g[msid.upper()])

    # Copy over all limits fields
    g[msid.upper()].update(limits)


def fill_states(tdb, g, msid):
    """ Fill in tdb expected state data where none are explicitly specified in G_LIMMON.

    :param tdb: TDB datastructure (corresponding to p012, p013, etc.)
    :param g: G_LIMMON datastructure (corresponding to a single version, e.g. 2.256)
    :param msid: Current MSID

    There is no return value, the "g" datastructure is updated in place.
    """

    states = assign_sets(tdb[msid]['exp_state'])

    states['type'] = 'expected_state'

    # Specify a default set.
    if is_not_nan(tdb[msid]['es_default_set_num']):
        states['default'] = tdb[msid]['es_default_set_num'] - 1
    else:
        states['default'] = 0

    # Alternate limit/es sets are automatically added to GLIMMON by GRETA, GLIMMON only adds MSIDs
    # to the current set of MSIDs and *prepends* limit/es sets that will take precedence
    # over sets defined in the database.
    if is_not_nan(tdb[msid]['es_switch_msid']):
        states['mlimsw'] = tdb[msid]['es_switch_msid']

        if 'es_switch' in list(tdb[msid].keys()):
            for setkey in states['setkeys']:  # assuming there are limit switch states for each set
                intkey = setkey + 1
                try:
                    states[setkey]['switchstate'] = tdb[msid]['es_switch'][intkey]['state_code']
                except:
                    states[setkey]['switchstate'] = 'none'

    for setkey in states['setkeys']:
        # if 'state_code' in states[setkey].keys():
        #     states[setkey]['switchstate'] = states[setkey]['state_code']
        #     _ = states[setkey].pop('state_code')

        if 'expected_state' in states[setkey]:
            # This could be listed as expst or expected_state, not sure why, make sure it is expst
            states[setkey]['expst'] = states[setkey]['expected_state']
            _ = states[setkey].pop('expected_state')

    # Fill in the default tolerance specified in the GLIMMON file
    if 'mlmtol' not in list(g[msid.upper()].keys()):
        states['mlmtol'] = g['mlmdeftol']

    # if mlmenable is not specified, set it to 1 (enabled) as a default
    if 'mlmenable' not in list(g[msid.upper()].keys()):
        states['mlmenable'] = 1

    # GLIMMON values take precedence over database values so if any are defined, update
    # the database dict with the GLIMMON fields
    states.update(g[msid.upper()])

    # Copy over all states fields
    g[msid.upper()].update(states)


def update_msid(msid, tdb, g):
    """ Call the appropriate function to fill in limits or expected states.

    :param msid: Current MSID
    :param tdb: TDB datastructure (corresponding to p012, p013, etc.)
    :param g: G_LIMMON datastructure (corresponding to a single version, e.g. 2.256)
    """
    if msid in list(tdb.keys()):
        if 'limit' in tdb[msid]:
            fill_limits(tdb, g, msid)
        elif 'exp_state' in tdb[msid]:
            fill_states(tdb, g, msid)


def raise_tabletype_error(tabletype):
    """ Raise error if table name does not match either 'limit' or 'expected_state'.

    :param tabletype: Name of table (i.e. type of table) that was attempted
    :raises ValueError: when wrong table name/type was attempted
    """
    s1 = "Argument 'tabletype' is entered as {}".format(tabletype)
    s2 = ", should be either 'limit' or 'expected_state'"
    raise ValueError("{}{}".format(s1, s2))


def query_all_cols_one_row_to_copy(db, msidset, tabletype):
    """ Return all columns for most recently defined limit or expected state data.

    :param db: sqlite3 database connection
    :param msidset: list containing msidname and set number
    :param tabletype: 'limit' or 'expected_state'

    :returns: list of column data for most recently defined limit or expected state data
    """
    cursor = db.cursor()
    if tabletype.lower() == 'limit':
        cursor.execute("""SELECT a.msid, a.setkey, a.datesec, a.date, a.modversion, a.mlmenable, a.mlmtol, a.default_set,
                          a.mlimsw, a.caution_high, a.caution_low, a.warning_high, a.warning_low, a.switchstate FROM limits AS a
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM limits AS b
                          WHERE a.msid=b.msid AND b.msid=? AND a.setkey=b.setkey and b.setkey=?) """, msidset)
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey, a.datesec, a.date, a.modversion, a.mlmenable, a.mlmtol, a.default_set,
                          a.mlimsw, a.expst, a.switchstate FROM expected_states AS a
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid=b.msid AND b.msid=? AND a.setkey=b.setkey and b.setkey=?) """, msidset)
    else:
        raise_tabletype_error(tabletype)
    return cursor.fetchone()


def query_most_recent_msids_sets(db, tabletype):
    """ Return all msid/set pairs defined in a table.

    :param db: sqlite3 database connection
    :param tabletype: 'limit' or 'expected_state'

    :returns: list of all msid/set pairs defined in a table.

    The database qurey code below selects the most recent disabled pair only as a way to filter
    out repeated data.

    """
    cursor = db.cursor()
    if tabletype.lower() == 'limit':
        cursor.execute("""SELECT a.msid, a.setkey FROM limits AS a 
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM limits AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) 
                          ORDER BY a.datesec, a.msid, a.setkey""")
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey FROM expected_states AS a 
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) 
                          ORDER BY a.datesec, a.msid, a.setkey""")
    else:
        raise_tabletype_error(tabletype)
    return cursor.fetchall()


def query_most_recent_disabled_msids_sets(db, tabletype):
    """ Return all msid/set pairs that have ever been disabled.

    :param db: sqlite3 database connection
    :param tabletype: 'limit' or 'expected_state'

    :returns: list of all msid/set pairs that have ever been disabled.

    The database qurey code below selects the most recent disabled pair only as a way to filter
    out repeated data.

    """
    cursor = db.cursor()
    if tabletype.lower() == 'limit':
        cursor.execute("""SELECT a.msid, a.setkey FROM limits AS a WHERE a.mlmenable = 0 AND
                          a.modversion = (SELECT MAX(b.modversion) FROM limits AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) 
                          ORDER BY a.datesec, a.msid, a.setkey""")
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey FROM expected_states AS a WHERE a.mlmenable = 0 AND
                          a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) 
                          ORDER BY a.datesec, a.msid, a.setkey""")
    else:
        raise_tabletype_error(tabletype)
    return cursor.fetchall()


def query_most_recent_changeable_data(db, tabletype):
    """ Return modifiable columns for limits or expected states for all msid set pairs.

    :param db: sqlite3 database connection
    :param tabletype: 'limit' or 'expected_state'

    :returns: modifiable columns for limits or expected states for all msid set pairs.

    The only columns that are omitted are the date, datesec, and modversion columns which are not
    edited by a standard user in the G_LIMMON file. This function returns the most recently
    defined rows regardless as to whether or not an msid/set pair have been disabled.

    """
    cursor = db.cursor()
    if tabletype.lower() == 'limit':
        cursor.execute("""SELECT a.msid, a.setkey, a.mlmenable, a.mlmtol, a.default_set, a.mlimsw, a.caution_high, 
                          a.caution_low, a.warning_high, a.warning_low, a.switchstate FROM limits AS a 
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM limits AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) 
                          ORDER BY a.datesec, a.msid, a.setkey""")
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey, a.mlmenable, a.mlmtol, a.default_set, a.mlimsw, a.expst, a.switchstate 
                          FROM expected_states AS a 
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) 
                          ORDER BY a.datesec, a.msid, a.setkey""")
    else:
        raise_tabletype_error(tabletype)
    return cursor.fetchall()
