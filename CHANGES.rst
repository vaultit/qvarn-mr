Change History
==============

0.1.8 (unreleased)
------------------


0.1.7 (2017-10-09)
------------------

- Added proper handler of duplicates for reduced resources, where single key
  could have more than one resource. Now newest resource is used and all other
  duplicates re automatically removed.

  Client applications must add `_mr_timestamp` field for all reduce target
  resource types.

- Added strict handler configuration validator. After upgrading you might get
  various `HandlerValidationError` exceptions. Simply read the explanation in
  the error message and fix what is needed.

- `qvarnmr.testing.utils.process` now has `raise_errors` keyword argument which
  is `True` by default. Previously, `process` helper simply logged errors from
  handlers, but did not propagated any errors. Now behaviour changed and by
  default errors are propagated. When running tests usually you want to get all
  the errors loudly.

- Added retries if client app handler fails with error. Retries are tracked per
  notification. Single notification can be handled by multiple handers, but if
  at least one handler fails, all handlers will be retries even those, who
  completed successfully.

  When an exception comes from handler, then notification is left undeleted and
  information about error is stored in memory. Then first retry will be
  attempted after 0.25 seconds, second after 1.5 second and finally
  notification will be deleted and no more retires will be attempted.

  If `qvarnmr-worker` will be terminated, then all notifications will be
  retries, but retries can happen more than 2 times, because information about
  previous retries where stored in RAM memory.

  This mechanism is temporary, until parallel handler processing will be
  implemented.


0.1.6 (2017-09-26)
------------------

- Fix issue, when reduce handler errors where not properly handled during full
  resync.

- qvarnmr.processed.process_map and _process_reduce functions became private
  functions, while qvarnmr.processed.MapReduceEngine.process_reduce_handlers
  became public, bicause it is used by resyncer.

- Add logging for how many items where produced by map handler.


0.1.5 (2017-09-04)
------------------

- Fixed one source for multiple targets bug, when listeners gave resource
  conflict errors.

- Replaced deprecated ``logger.warn`` to ``logger.warning``.


0.1.4 (2017-08-31)
------------------

- Proper logging

- Configurable keep alive parameters

- Keep alive updates after each handler processing

- Fix reduce full resync bug


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
