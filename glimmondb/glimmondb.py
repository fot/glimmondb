
import sqlite3
import pickle as pickle
import shutil
import errno
from os.path import join as pathjoin
from os import getenv, getcwd
import numpy as np
import re
import logging
import imp
imp.reload(logging)  # avoids issue with ipython notebook

from Chandra.Time import DateTime

if getenv('GLIMMONDATA'):
    DBDIR = getenv('GLIMMONDATA')
elif getenv('SKA_DATA'):
    DBDIR = pathjoin(getenv('SKA_DATA'), 'glimmon_archive/')
else:
    DBDIR = getcwd()

if getenv('TDBDATA'):
    TDBDIR = getenv('TDBDATA')
elif getenv('SKA_DATA'):
    TDBDIR = pathjoin(getenv('SKA_DATA'), 'fot_tdb_archive/')
else:
    TDBDIR = getcwd()

logfile = pathjoin(DBDIR, 'DB_Commit.log')
# try:
#     with open(logfile) as fid:
#         pass
# except IOError:
#     mknod(logfile)

logging.basicConfig(filename=logfile, level=logging.DEBUG,
                    format='%(asctime)s %(message)s')


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
                  'p014': '2015:239:20:00:00'}

    if return_dates:
        return startdates

    else:
        if not tdbs:
            tdbfile = pathjoin(TDBDIR, "tdb_all.pkl")
            tdbs = pickle.load(open(tdbfile, 'rb'))

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
        elif revision <= 999:  # p014 starts with 261 and ends with ?
            print ('Using P014')
            tdb = tdbs['p014']
        return tdb


def read_glimmon(filename='/home/greta/AXAFSHARE/dec/G_LIMMON.dec'):
    """ Import G_LIMMON.dec file

    :param filename: Name/Location of G_LIMMON file to import

    :returns: Dictionary containing the import G_LIMMON information

    """

    revision_pattern = '.*\$Revision\s*:\s*([0-9.]+).*$'
    date_pattern = '.*\$Date\s*:\s*([0-9]+)/([0-9]+)/([0-9]+)\s+([0-9]+):([0-9]+):([0-9]+).*$'
    version_pattern = '.*Version\s*:\s*[$]?([A-Za-z0-9.:\s*]*)[$]?"\s*$'
    database_pattern = '.*Database\s*:\s*(\w*)"\s*$'

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
                version = re.findall(revision_pattern, line)
                glimmon.update({'revision': version[0].strip()})
                glimmon.update({'version': version[0].strip()})

            elif len(re.findall('^XMSID TEXTONLY ROWCOL.*COLOR.*Version', line)) > 0:
                version = re.findall(version_pattern, line)
                glimmon.update({'version': version[0].strip()})

            elif len(re.findall('^XMSID TEXTONLY ROWCOL.*COLOR.*Database', line)) > 0:
                database = re.findall(database_pattern, line)
                glimmon.update({'database': database[0].strip()})

        # elif len(re.findall('^#\$Revision', comment_line)) > 0:
        elif len(re.findall(revision_pattern, comment_line)) > 0:
            revision = re.findall(revision_pattern, comment_line)
            glimmon.update({'revision': revision[0].strip()})

        # elif len(re.findall('^#\$Date', comment_line)) > 0:
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
                charkey = str(setkey + 1)
                try:
                    limits[setkey]['switchstate'] = tdb[msid]['lim_switch'][charkey]['state_code']
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
                charkey = str(setkey + 1)
                try:
                    states[setkey]['switchstate'] = tdb[msid]['es_switch'][charkey]['state_code']
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
        removekeys = ['revision', 'version', 'mlmdeftol', 'mlmthrow', 'database', 'date']
        for key in removekeys:
            self.__setattr__(key, self.gdb[key])
            msids.remove(key)

        # MSIDs that have no type will not have any defined limits/expected states (e.g. coscs098s)
        for msid in msids:
            if 'type' not in list(self.gdb[msid].keys()):
                msids.remove(msid)
        return msids

    def gen_limit_row_data(self):
        """ Return G_LIMMON limit definitions in row format.

        The returned list will be structured to be compatible with the final sqlite3 limit
        definition table.
        """
        limitrowdata = []
        for msid in self.msids:
            #     print(msid)
            mdata = self.gdb[msid]
            if mdata['type'].lower() == 'limit':
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

        # Remove disabled MSID rows, this will be taken care of later when these
        # are found to be missing
        if self.discard_disabled_sets:
            for n, row in enumerate(limitrowdata):
                if int(row[5]) == 0:
                    _ = limitrowdata.pop(n)

        return limitrowdata

    def gen_state_row_data(self):
        """ Return G_LIMMON expected state definitions in row format.

        The returned list will be structured to be compatible with the final sqlite3 expected
        state definition table.
        """

        esstaterowdata = []
        for msid in self.msids:
            mdata = self.gdb[msid]
            if mdata['type'].lower() == 'expected_state':
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

        # Remove disabled MSID rows, this will be marked as disabled later when
        # these are found to be missing
        if self.discard_disabled_sets:
            for n, row in enumerate(esstaterowdata):
                if int(row[5]) == 0:
                    _ = esstaterowdata.pop(n)

        return esstaterowdata

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
                          WHERE a.msid = b.msid and a.setkey = b.setkey) """)
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey FROM expected_states AS a 
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) """)
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
                          WHERE a.msid = b.msid and a.setkey = b.setkey) """)
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey FROM expected_states AS a WHERE a.mlmenable = 0 AND
                          a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) """)
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
                          WHERE a.msid = b.msid and a.setkey = b.setkey) """)
    elif tabletype.lower() == 'expected_state':
        cursor.execute("""SELECT a.msid, a.setkey, a.mlmenable, a.mlmtol, a.default_set, a.mlimsw, a.expst, a.switchstate 
                          FROM expected_states AS a 
                          WHERE a.modversion = (SELECT MAX(b.modversion) FROM expected_states AS b
                          WHERE a.msid = b.msid and a.setkey = b.setkey) """)
    else:
        raise_tabletype_error(tabletype)
    return cursor.fetchall()


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
            logging.info('    Added new row:{}.'.format(row))

    elif tabletype.lower() == 'expected_state':
        for row in rows:
            oldcursor.execute("""INSERT INTO expected_states(msid, setkey, datesec, date, modversion, mlmenable, 
                                 mlmtol, default_set, mlimsw, expst, switchstate) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", row)
            logging.info('    Added new row:{}.'.format(row))
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

    oldcursor = olddb.cursor()
    oldcursor.execute(
        """INSERT INTO versions(version, datesec, date) VALUES(?,?,?)""", (version, datesec, date))
    olddb.commit()


def add_merge_logging_text(tabletype, functiontype, newlen, oldlen, addlen):
    """ Add Merge Operation Logging.

    :param tabletype: either 'limit' or 'expected_state'
    :param functiontype: either 'added', 'deactivated', or 'modified'

    """

    formatted_tabletype = ' '.join([s.capitalize() for s in tabletype.split('_')])
    formatted_functiontype = functiontype.capitalize()
    logging.info('========== Comparing New and Current {}s Table For {} MSID Sets =========='
                 .format(formatted_tabletype, formatted_functiontype))
    logging.debug('Number of {} rows in new DB:{}.'.format(formatted_tabletype, newlen))
    logging.debug('Number of {} rows in current DB:{}.'.format(formatted_tabletype, oldlen))
    logging.info('Number of rows to be {} for newly added msid sets: {}.'
                 .format(formatted_functiontype, addlen))
    logging.info('---------- Append Rows For {} MSID Sets ----------'.format(formatted_tabletype))


def merge_added_msidsets(newdb, olddb, tabletype):
    """ Merge new/modified data to the current database.

    :param olddb: sqlite3 database connection for old database (current database to be updated)
    :param newdb: sqlite3 database connection for new database (sqlite version of newest G_LIMMON)
    :param tabletype: type of table ('limit' or 'expected_state')

    """

    all_new_rows = query_most_recent_msids_sets(newdb, tabletype)
    all_old_rows = query_most_recent_msids_sets(olddb, tabletype)

    # what is in newrows but not in oldrows
    not_in_oldrows = set(all_new_rows).difference(set(all_old_rows))
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

    disabled_not_in_newrows = set(all_old_disabled_rows).difference(set(all_new_rows))
    all_new_rows.extend(list(disabled_not_in_newrows))

    # what is in oldrows but not in newrows
    not_in_newrows = set(all_old_rows).difference(set(all_new_rows))
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
    modified_rows = set(all_new_rows).difference(set(all_old_rows))
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
    esstaterowdata2 = g.gen_state_row_data()
    limitrowdata2 = g.gen_limit_row_data()
    version = int(str(gdb['revision'])[2:])
    newdb_obj = NewLimitDB(limitrowdata2, esstaterowdata2, version, g.date, g.datesec)
    return newdb_obj.db


def recreate_db(glimmondbfile='glimmondb.sqlite3'):
    """ Recreate the G_LIMMON history sqlite3 database from all G_LIMMON.dec past versions.

    :param glimmondbfile: Filename for glimmon database, defaults to 'glimmondb.sqlite3'

    """

    from operator import itemgetter
    import glob

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

        logging.info(
            '========================= glimmondb.sqlite3 Initialized =========================\n')

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
    tdbs = pickle.load(open(tdbfile, 'rb'))
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
    logging.info('')
    logging.info('                        ========== {} =========='.format(textinsert))

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

    newdb.close()
    olddb.close()

    temp_filename = pathjoin(DBDIR, 'temporary_db.sqlite3')
    try:
        shutil.rmtree(temp_filename)
    except OSError:
        pass
