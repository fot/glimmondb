import hashlib
import re
import glob
import shutil
import logging
import time
from os.path import expanduser
from os.path import join as pathjoin
import pickle as pickle

import glimmondb

# home = expanduser("~")
# archive = pathjoin(home, "AXAFAUTO/G_LIMMON_Archive/")
# tdbfile = pathjoin(home, "AXAFAUTO/TDB_Archive/tdb_all.pkl")
archive = glimmondb.DBDIR
tdbfile = pathjoin(glimmondb.TDBDIR, "tdb_all.pkl")


def get_date():
    t = time.gmtime()
    return "{}:{}:{}:{}:{}".format(t.tm_year, t.tm_yday, t.tm_hour, t.tm_min, t.tm_sec)


def get_max_arch_revision(glimmon_files):
    filename_rev_pattern = 'G_LIMMON_([0-9]+).([0-9]+).dec'
    maxrev = [0, 0]
    for filename in glimmon_files:
        rev = re.findall(filename_rev_pattern, filename)[0]
        rev = [int(n) for n in rev]
        if rev[0] >= maxrev[0]:
            if rev[1] > maxrev[1]:
                maxrev = rev
    return maxrev


def get_glimmon_revision(lines):
    glimmon_rev_pattern = '^#\$Revision\s*:\s*([0-9]+).([0-9]+).*$'
    return [int(n) for n in re.findall(glimmon_rev_pattern, lines[0])[0]]


def get_glimmon_hash(lines):
    m = hashlib.md5()
    m.update("".join(lines))
    return m.hexdigest()


def read_glimmon_file(newfilename):
    with file(newfilename, "r") as fid:
        lines = fid.readlines()
    return lines


def get_glimmon_arch_filenames():
    glimmon_files = glob.glob(archive + "G_LIMMON_2.*.dec")
    return glimmon_files


def check_and_copy_file(new_revision, arch_revision):
    if new_revision[0] >= arch_revision[0]:
        if new_revision[1] > arch_revision[1]:
            targetfile = "{}G_LIMMON_{}.{}.dec".format(archive, new_revision[0], new_revision[1])
            sourcefile = "/home/greta/AXAFSHARE/dec/G_LIMMON.dec"
            shutil.copy2(sourcefile, targetfile)


if __name__ == '__main__':
    logging.basicConfig(filename=pathjoin(archive, 'glimmon_cron.log'), level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    datestring = get_date()
    logging.info(
        '========================= G_LIMMON check on {} ========================='.format(datestring))

    new_filename = "/home/greta/AXAFSHARE/dec/G_LIMMON.dec"
    new_glimmon_lines = read_glimmon_file(new_filename)
    new_hash = get_glimmon_hash(new_glimmon_lines)
    new_revision = get_glimmon_revision(new_glimmon_lines)
    logging.info('Current G_LIMMON revision is {}.{}'.format(new_revision[0], new_revision[1]))
    logging.debug('Current G_LIMMON MD5 hash is {}'.format(new_hash))

    glimmon_arch_files = get_glimmon_arch_filenames()
    max_arch_revision = get_max_arch_revision(glimmon_arch_files)
    logging.info('Latest Archived G_LIMMON revision is {}.{}'.format(
        max_arch_revision[0], max_arch_revision[1]))

    arch_filename = "{}G_LIMMON_{}.{}.dec".format(
        archive, max_arch_revision[0], max_arch_revision[1])
    arch_glimmon_lines = read_glimmon_file(arch_filename)
    arch_hash = get_glimmon_hash(arch_glimmon_lines)
    arch_revision = get_glimmon_revision(arch_glimmon_lines)
    logging.debug('Latest Archived G_LIMMON MD5 hash is {}'.format(arch_hash))

    # Revision in filename should match internally recorded revision
    if max_arch_revision[1] != arch_revision[1]:
        text1 = "Most recent archived revision in filename ({}.{})".format(
                max_arch_version[0], max_arch_version[1])
        text2 = "does not match internally recorded revision number ({}.{})".format(
                arch_revision[0], arch_revision[1])
        logging.warning("{}{}".format(text1, text2))

    if new_hash != arch_hash:
        newfile = "{}G_LIMMON_{}.{}.dec".format(archive, new_revision[0], new_revision[1])
        shutil.copy2("/home/greta/AXAFSHARE/dec/G_LIMMON.dec", newfile)
        logging.info('Current G_LIMMON revision {}.{} added to archive'.format(
            new_revision[0], new_revision[1]))
        print("New G_LIMMON.dec file copied")

        tdbs = pickle.load(open(tdbfile, 'r'))
        glimmondb.merge_new_glimmon_to_db(newfile, tdbs)
        print("New G_LIMMON.dec merged into sqlite database")

    else:
        logging.info('Current G_LIMMON revision {}.{} already in archive'.format(
            new_revision[0], new_revision[1]))
        print("G_LIMMON.dec archive up to date")
