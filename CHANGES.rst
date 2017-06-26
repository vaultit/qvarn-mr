Change History
==============

0.1.1 (unreleased)
------------------

- ``qvarnmr-resync`` script was replaced by automatic synchronisation. So now,
  it is enought to just run ``qvarnmr-worker`` and all the sinchronisation will
  happen automatically. You just need to restart ``qvarnmr-worker`` after
  updateing handlers configuration.

- New qvarn-mr resource type ``qvarnmr_handlers`` was added:

  .. code-block:: yaml

      path: /qvarnmr_handlers
      type: qvarnmr_handler
      versions:
      - prototype:
          id: ''
          type: ''
          revision: ''
          instance: ''
          target: ''
          source: ''
          version: 0
        version: v1

- Derived map resource types has two more fields::

      _mr_version: 0
      _mr_deleted: false

- Derived reduce resource types has one more field::

      _mr_version: 0

- Refactored map/reduce handlers configuration structure, now target <- source
  definitions are unique. Also ``version`` and ``type`` fields added and
  ``map`` and ``reduce`` fields renamed to ``handler``. Previously it looked
  like this::

      {
          'map_target': [
              {
                  'source': 'resource_name',
                  'map': item('id'),
              },
          ],
          'reduce_target': [
              {
                  'source': 'map_target',
                  'reduce': join(),
              },
          ],
      }

  Now it should look like this::

      {
          'map_target': {
              'resource_name': {
                  'type': 'map',
                  'version': 1,
                  'handler': item('id'),
              },
          ],
          'reduce_target': [
              'map_target': {
                  'type': 'reduce',
                  'version': 1,
                  'handler': join(),
              },
          ],

      }

- Remove test dependencies from ``setup.py``.

- Upgrade qvarn-mr to support Qvarn 0.82.


0.1.0 (2017-05-17)
------------------

- Initial version, tested with Qvarn 0.80.
