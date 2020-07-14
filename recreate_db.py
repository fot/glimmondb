from os.path import join as pathjoin
from os import environ, mkdir, rmdir, getenv, mknod, remove
from sqlite3 import connect
from hashlib import sha256
from shutil import copytree, rmtree, copy
from Chandra.Time import DateTime

environ["GLIMMONDATA"] = "./testing_data"

import glimmondb

glimmondb.recreate_db(glimmondbfile='glimmondb.sqlite3')
