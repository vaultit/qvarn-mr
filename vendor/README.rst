Vendored packages
=================

Our builds require some packages that are not publically available.


qvarn-utils
-----------

Private git repository: https://git.vaultit.org/common/qvarn-utils

Update procedure:

#. ``cd /path/to/qvarn-utils``
#. edit ``setup.py`` and change version to match the latest version in
   ``debian/changelog`` and add ``+gitYYYYMMDD.commitsha`` at the end
#. ``python3 setup.py sdist``
#. copy ``dist/*.tar.gz`` to ``/path/to/bolagsfakta/server/vendor/``
#. ``cd /path/to/bolagsfakta/``
#. run ``make -C server update-requirements`` to update version pin in requirements.txt
#. maybe edit the various requirements files and undo upgrades of other packages
#. run ``make -C server test`` to verify that it works
#. commit


qvarn
-----

Qvarn package is only used for tests, by bolfak.testing.realqvarn to test more
complicated Qvarn usage cases, like map/reduce.

Qvarn package used here is a Python 3 fork of upstream Qvarn with applied
patches to add support for Python 3.

Current package ``qvarn-0.82+py3.tar.gz`` was built using this branch:

https://git.vaultit.org/qvarn/qvarn/tree/qvarn-py3-0.82

When new Qvarn is released, new branch ``qvarn-py3-<qvarn.version>`` should be
created, and rebased on release version tag.

Update procedure:

1. Clone qvarn and checkout ``qvarn-py3-<version>`` branch::

      git clone ssh://git@git.vaultit.org:23/qvarn/qvarn.git qvarn-py3

      cd qvarn-py3

      git checkout qvarn-py3-0.82

2. Build package::

      python3.4 setup.py sdist

If you want update the fork to a newer Qvarn release, after rebasing on top of
a newer release, don't forget to run tests, to make sure that everything works.
Instructions how to run tests can be found here:

https://github.com/ProgrammersOfVilnius/qvarn-testing

If you change anything in the ``qvarn-py3-*`` branches, don't forget to push
changes to the open source mirror:

https://github.com/vaultit/qvarn
