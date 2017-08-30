Change History
==============

0.1.3 (unreleased)
------------------

- Now keep alive updates are done after each map/reduce handler processing.
  Previously keep alive updates happend only after each map/reduce changes
  batch.

- Keep alive update automatically refeshes state resource from Qvarn if time
  from alast update is bigger than timeout.

- process_changes function was refactored to MapReduceEngine.process_changes
  method.

- Proper logging was added, now it is possible to see what is happening inside
  qvarn-mr by looking at logs.

- Fix full reduce resync bug, it seems, that full reduce resync was blockin
  whole map/reduce process while resyncing.

- Configurable keep alive parameters using **keep_alive_update_interval** and
  **keep_alive_timeout** configuration file options.

0.1.2 (2017-07-14)
------------------

- Added checks to make sure that one worker is running. This required some
  schema changes, two fields ``owner`` and ``timestamp`` where added to
  ``qvarnmr_handlers`` resource type.

0.1.1 (2017-06-27)
------------------

- Reduce handlers now are processed using Qvarn notifications. This should
  solve possible cases related with eventual consistency.

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
