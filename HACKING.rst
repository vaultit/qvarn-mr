Coding style
============

PEP-8 and `NumPy Style docstrings`_.


.. _NumPy Style docstrings: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html#example-numpy


How to release new version
==========================

- Update version number in setup.py.

- Run ``python setup.py sdist``.

- Add git tag::

    git tag -a 0.1.2 -m "New qvarn-mr release (0.1.2)"

- Push canges and the tag::

    git push --tags
    
