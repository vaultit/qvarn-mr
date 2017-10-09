Coding style
============

PEP-8 and `NumPy Style docstrings`_.


.. _NumPy Style docstrings: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html#example-numpy


How to release new version
==========================

- Update version number in ``setup.py`` and ``debian/changelog``.

- Run ``make sdist``.

- Commit changes with message "New qvarn-mr release (0.1.2)".

- Add git tag::

    git tag -a 0.1.2 -m "New qvarn-mr release (0.1.2)"

- Push canges and the tag::

    git push --tags
    
