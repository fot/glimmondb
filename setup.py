from distutils.core import setup
from glimmondb.version import version

long_description = """
A Python package that compiles an SQLite 3 database of the entire G_LIMMON.dec history of changes.
This package also includes the necessary tools for maintaining this database as new versions of
G_LIMMON.dec are promoted to flight status.
"""

setup(name='glimmondb',
      version=version,
      description='Generates an SQLite 3 database of G_LIMMON.dec changes.',
      long_description=long_description,
      author='Matthew Dahmer',
      author_email='mdahmer@ipa.harvard.edu',
      packages=['glimmondb',],
      )