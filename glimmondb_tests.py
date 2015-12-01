from os.path import join as pathjoin
from os import environ, mkdir, rmdir, getenv, mknod, remove
from sqlite3 import connect
from hashlib import sha256
from shutil import copytree, rmtree, copy

from Chandra.Time import DateTime

# Set this for now, this should not be necessary in production
environ["SKA_DATA"] = "/home/mdahmer/AXAFAUTO/"
environ["TDBDATA"] = "/home/mdahmer/AXAFAUTO/TDB_Archive/"


try:
    rmtree('./testing_data')
except:
    print('Could not delete testing folder, it probably did not exist')

environ["GLIMMONDATA"] = "./testing_data"
# copytree(pathjoin(getenv('SKA_DATA'), 'glimmon_archive/'), './testing_data/')
copytree(pathjoin(getenv('SKA_DATA'), 'G_LIMMON_Archive/'), './testing_data/')
try:
    remove('./testing_data/DB_Commit.log')
except:
    print 'Could not delete old log'
import glimmondb

print('Regenerated testing folder at: ./testing_data')
print('Using G_LIMMON Archive at: {}'.format(glimmondb.DBDIR))
print('Using TDB Archive at: {}'.format(glimmondb.TDBDIR))

# mknod('./testing_data/DB_Commit.log')

def querydatabase(glimmondbfile, datecheckbefore):
    db = connect(glimmondbfile)
    cursor = db.cursor()

    cursor.execute('''SELECT msid, setkey, datesec, date, modversion, mlmenable, mlmtol, 
                      default_set, mlimsw, caution_high, caution_low, warning_high, warning_low, 
                      switchstate FROM limits WHERE datesec < ?''', (datecheckbefore,))
    all_limits = cursor.fetchall()

    cursor.execute('''SELECT msid, setkey, datesec, date, modversion, mlmenable, mlmtol, default_set,
                      mlimsw, expst, switchstate FROM expected_states WHERE datesec < ?''',
                      (datecheckbefore,))
    all_states = cursor.fetchall()

    cursor.execute('''SELECT version, datesec, date FROM versions WHERE datesec < ?''',
                      (datecheckbefore,))
    all_versions = cursor.fetchall()

    db.close()

    return all_limits, all_states, all_versions


def gethashes(all_limits, all_states, all_versions):
    limithash = sha256(unicode(all_limits)).hexdigest()
    statehash = sha256(unicode(all_states)).hexdigest()
    versionhash = sha256(unicode(all_versions)).hexdigest()
    return limithash, statehash, versionhash



def saveoutputs(all_limits, all_states, all_versions, prestr='new'):
    with open(pathjoin(glimmondb.DBDIR, '{}_limits_data.txt'.format(prestr)), 'w') as fid:
        for row in all_limits:
            fid.write('{}\n'.format(unicode(row)))

    with open(pathjoin(glimmondb.DBDIR, '{}_states_data.txt'.format(prestr)), 'w') as fid:
        for row in all_states:
            fid.write('{}\n'.format(unicode(row)))

    with open(pathjoin(glimmondb.DBDIR, '{}_versions_data.txt'.format(prestr)), 'w') as fid:
        for row in all_versions:
            fid.write('{}\n'.format(unicode(row)))



def test_function():
    datecheckbefore = DateTime('2015:321:00:00:00').secs
    oldlimithash = '92b6b2069bb64b0c784d7755370f7c0dc6df9c8b6da0e817bc77fdfaa8225d64'
    oldstatehash = 'c4f5ee8ad7d7a11054196f9c91ebc3751d799816d60cf0667c3d9f2777d7f8db'
    oldversionhash = '94eedd6f4fa203daadb62746c3dbc85e6f154abcd448c69c58a7f7e2f8309a31'

    glimmondb.recreate_db(glimmondbfile='glimmondb_testing.sqlite3')
    copy('./testing_data/glimmondb_testing.sqlite3', './glimmondb_testing_backup.sqlite3')

    new_all_limits, new_all_states, new_all_versions = querydatabase(pathjoin(glimmondb.DBDIR, 
        'glimmondb_testing.sqlite3'), datecheckbefore)

    old_all_limits, old_all_states, old_all_versions = querydatabase(pathjoin(glimmondb.DBDIR, 
        'glimmondb.sqlite3'), datecheckbefore)

    saveoutputs(new_all_limits, new_all_states, new_all_versions, prestr='new')
    saveoutputs(old_all_limits, old_all_states, old_all_versions, prestr='new')

    newlimithash, newstatehash, newversionhash = gethashes(new_all_limits, new_all_states, new_all_versions)
    oldlimithash, oldstatehash, oldversionhash = gethashes(old_all_limits, old_all_states, old_all_versions)

    print('newlimithash = {}\nnewstatehash = {}\nnewversionhash = {}'.format(newlimithash, newstatehash, newversionhash))
    print('oldlimithash = {}\noldstatehash = {}\noldversionhash = {}'.format(oldlimithash, oldstatehash, oldversionhash))

    assert oldlimithash == newlimithash
    assert oldstatehash == newstatehash
    assert oldversionhash == newversionhash

    print('Test Completed {}'.format(DateTime().caldate))

