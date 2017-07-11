Experimental Qvarn map/reduce service
#####################################

The idea is to create a service, that will listen for changes on specified
resource types. All changes will be routed through defined map/reduce handlers.
And the output will be written into derived resource types.

This service should allow to join several resources into one resource type and
do aggregations via reduce functions.

You can read more about map/reduce in the Wikipedia_.

.. _Wikipedia: https://en.wikipedia.org/wiki/MapReduce


Status
======

The service is fully implemented and has near 100% test coverage, but is not
yet used by any real project. So theoretically it should work, but practice it
was not yet tested. So first integrators should expect rough edged.

The code is very basic and currently does not deal with huge amounts of data.
It just takes list of all available resource ids and process each id in a
synchronous way, one by one. This part should be improved, but if list of all
resource type ids fits into RAM, it should work fine.

Also qvarn-mr takes care of all synchronizations things automatically. That
means, when you deploy qvarn-mr for the first time, it will do initial
synchronization automatically, when new map/reduce handlers are added, they
will be updated automatically.


How to install
==============

The code was tested using python 3.4.5 and pip 9.0.1.

In order to install qvarnmr, create a virtualenv, activate it and run these
commands::

  pip install -f vendor -r requirements.txt -e . 

Yes this is install from source. Currently there is no any python package, nor
debian package. But python source package should be easily crated by running::

  python setup.py sdist

Then you can put this package to your project and install it from there.


How to run tests
================

Activate your virtualenv and run::

  py.test --cov-report=term-missing --cov=qvarnmr tests


How to use qvarnmr
==================

Firs of all, you have to define map/reduce handlers. You can read more about
how to define map/reduce handlers in the `How to define map/reduce handlers`_
section. Here is just a simple example:

.. code-block:: python

    from qvarnmr.func import join, item


    handlers = {
        'company_reports__map': {
            'orgs': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
            'reports': {
                'type': 'map',
                'version': 1,
                'handler': item('org'),
            },
        },
        'company_reports': {
            'company_reports__map': {
                'type': 'reduce',
                'version': 1,
                'handler': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        },
    }

Handlers should be defined in an importable Python file.

Then you have to define new derived Qvarn resource types in Qvarn resource
types yaml file. ``qvarn-mr`` also requires two resource tipes to manage
internal state:

.. code-block:: yaml

    path: /qvarnmr_listeners
    type: qvarnmr_listener
    versions:
    - prototype:
        id: ''
        type: ''
        revision: ''
        instance: ''
        resource_type: ''
        listener_id: ''
      version: v1

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

Once you have defined handlers and new resource types, you run
``qvarnmr-worker``::

  qvarnmr-worker path.to.handlers -c path/to/qvarnmr.cfg -f

Here ``path.to.handlers`` is a Python path to your map/reduce handlers
configuration.

``-c path/to/qvarnmr.cfg`` is a Python configparser configuration file. Here is
example of configuration file:

.. code-block:: ini

    [qvarn]
    base_url = https://qvarn-example.tld,
    client_id = test_client_id
    client_secret = verysecret
    verify_requests = no
    scope = scope1,scope2,scope3

    [qvarnmr]
    instance = instance-name

In this configuration file you need to specify connection parameters for the
Qvarn. Also you need to specify qvarnmr instance name. This name will be used
to know which notification handlers to use. There can be multiple qvarnmr
instances running, each processing different handlers. In order to distinguish
between these qvarnmr instances, instance name is used.

Probably it's a good idea to use project domain name as instance name. But
basically it can be anything, just make sure, that two instances does not have
the same name, because then each will steal notifications from one another.

``-f`` stands for *forever*.

That's it.


How to define map/reduce handlers
=================================

Let's analyse the following example a bit further:

.. code-block:: python

    from qvarnmr.func import join, item


    handlers = {
        'company_reports__map': {
            'orgs': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
            'reports': {
                'type': 'map',
                'version': 1,
                'handler': item('org'),
            },
        },
        'company_reports': [
            'company_reports__map': {
                'type': 'reduce',
                'version': 1,
                'handler': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        },
    }

Here we have two **source resource types** ``orgs`` and ``reports`` for the
**map handler**. Result of these map functions will be written to a new derived
resource type ``company_reports__map``, we will call these resource types as
**target resource types**.

Then everything, that goes  into ``company_reports__map`` will be processed by
the **reduce handler**, key by key. Result of the reduce handler will be
written into ``company_reports`` target resource type.

``item()`` and ``join()`` are helpers, to build a function for handling common
map/reduce tasks, like taking an item from source resource or joining multiple
resources into one. For such common tasks you don't need to define custom map
or reduce functions, you can use these helpers.

If there is no helper, you can always use your own functions.

How to define map function
--------------------------

Here is example of a map function:

.. code-block:: python

    def my_map_function(resource):
        return resource['id'], None

Each map function receives single argument, a resource. Each map function can
simple return or yield (**key**, **value**) tuple.

**value** can be a ``None``, a scalar value or a dict. If **value** is a dict,
then it will be interpreted as a resource. If **value** is not a dict, then it
will be stored in the ``_mr_value`` field of the resource.

In cases, when you want to get more control you can decorate your map (or
reduce) function with ``qvarnmr.func.mr_func`` decorator. For example:

.. code-block:: python

    from qvarnmr.func import mr_func

    @mr_func()
    def orgs_users(context, resource):
        for contract in context.qvarn.search('contracts', resource_id=resource['id']):
            person = contract.get_one('contract_parties', role='user')
            yield resource['id'], person['id']

With ``@mr_func()`` decorator your map function will get ``context`` argument.
Context is a namedtuple and with following fields:

- ``qvarn`` - ``QvarnApi`` instance for accessing Qvarn database.
- ``source_resource_type`` - source resource type.


How to define reduce function
-----------------------------

Reduce functions are very similar to the map functions, except reduce will get
generator of **resource ids** as a first argument. Note, that you will get
generator of **resource ids**, not full resources.

For example, in order to get number of resources for each key yielded by a map
function, you can simply pass ``qvarnmr.func.count`` as reduce function.
Handler definition will look like this:

.. code-block:: python

    from qvarnmr.func import count

    {
        'type': 'reduce',
        'version': 1,
        'handler': count,
    },

We can't use ``len`` here, because first argument is a generator, not a list.
That's why there is a ``count`` function, that will consume the generator and
returns number of generated items.

If you want to access whole resource by its id, you have to do something like this:

.. code-block:: python

    @mr_func()
    def count_something_else(context, resources):
        resources = context.qvarn.get_multiple(context.source_resource_type, resources)
        return sum(resource['something_else'] for resource in resources)

And the handler definition would look like this:

.. code-block:: python

    {
        'type': 'reduce',
        'version': 1,
        'handler': count_something_else(),
    },

To achieve same thing, you can also use ``map`` function for reduce handler,
like this:

.. code-block:: python

    from qvarnmr.func import value

    {
        'type': 'reduce',
        'version': 1,
        'handler': sum,
        'map': value('something_else'),
    },

Here, first argument for reduce function will be processed with
``value('something_else')``, which simply fetches the source resource and
returns value of ``something_else``.

If you want access source resource used by map function, which in turn is used
as a source for reduce function, then you can do something like this:

.. code-block:: python

    @mr_func()
    def count_something_from_map_source(context, resources):
        resources = context.qvarn.get_multiple(context.source_resource_type, resources)
        resources = (qvarn.get(x['_mr_source_type'], x['_mr_source_id']) for x in resources)
        return sum(x['value'] for x in resources)



How to define derived resource types
====================================

When defining new resource types for map/reduce results, you need to define
some special fields used by qvarnmr engine.

For map target resource type, these fields are required:

.. code-block:: yaml

    path: /derived_map_resources
    type: derived_map_resource
    versions:
    - prototype:
        id: ''
        type: ''
        revision: ''
        _mr_key: ''
        _mr_value: ''
        _mr_source_id: ''
        _mr_source_type: ''
        _mr_version: 0
        _mr_deleted: false
      version: v1

Purpose of these fields:

- ``_mr_key`` - is a key, yielded by map function.

- ``_mr_value`` - if map functions yields a dict, this will be None, otherwise
  it will contain yielded value.

- ``_mr_source_id`` - resource id of a source resource type, this is needed to
  track resource updates and deletes.

- ``_mr_source_type`` - source resource type, this is needed to track resource
  updates and deletes.

- ``_mr_version`` - handler version used to produce data for this resource.
  Version is used to do automatic updates if handler version has changed.

- ``_mr_deleted`` - used for internal purposes, to track which resources have
  to be deleted, once whole update cycle is done. Resources are not deleted
  immediately in order to be able to recover in case of an error in the middle
  of map/reduce function execution.

For reduce target resource type, these fields are required:

.. code-block:: yaml

    path: /derived_reduce_resources
    type: derived_reduce_resource
    versions:
    - prototype:
        id: ''
        type: ''
        revision: ''
        _mr_key: ''
        _mr_value: ''
        _mr_version: 0
      version: v1

Purpose of these fields:

- ``_mr_key`` - is a key that represents group of values produced by map
  function with same key.

- ``_mr_value`` - same as with map, if reduce value is not a dict, then value
  will be assigned to this field.

- ``_mr_value`` - purpose of this field is exactly the same as for map derived
  resources.


Helper functions
================

count
-----

Count number of items.


item(key, value=None)
---------------------

Return key and value from a resource, by specified key and value field names.

Example:

.. code-block:: python

  >>> handler = item('foo')
  >>> handler({'foo': 'key', 'bar': 42})
  ('key', None)

  >>> handler = item('foo', 'bar')
  >>> handler({'foo': 'key', 'bar': 42})
  ('key', 42)


value(key='_mr_value')
----------------------

Return a field value from a resource by specified field name.

Example:

.. code-block:: python

  >>> handler = value()
  >>> handler({'_mr_value': 1, 'foo': 42})
  1

  >>> handler = value('foo')
  >>> handler({'_mr_value': 1, 'foo': 42})
  42


join(mapping)
-------------

Should by useful for reduce handlers. Accepts a mapping of source resource
types and field mapping for each resource type. The result is a dict joined
from list of source resources.

.. code-block:: python

  >>> handler = value({
  ...     'a': {
  ...         'id': 'a_id',
  ...         'foo': None,
  ...     },
  ...     'b': {
  ...         'id': 'b_id',
  ...         'bar': None,
  ...     },
  ... })
  >>> handler([
  ...     {'type': 'a', 'id': 1, 'foo': 42},
  ...     {'type': 'b', 'id': 2, 'bar': 24},
  ... ])
  {'a_id': 1, 'b_id': 2, 'foo': 42, 'bar': 24}

This example is not exactly true, because ``handler`` will get generator of map
target resource type ids, but join handler will fetch resource for each id and
then for each resource it will fetch source resource and then will do the
mapping.

For example, if we have following handler configuration:

.. code-block:: python

    from qvarnmr.func import join, item


    handlers = {
        'company_reports__map': {
            'orgs': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
            'reports': {
                'type': 'map',
                'version': 1,
                'handler': item('org'),
            },
        ],
        'company_reports': [
            'company_reports__map': {
                'type': 'reduce',
                'version': 1,
                'reduce': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        },
    }

Then ``company_reports__map`` will have something like this::

    [
        {
            'id': 1,
            'type': 'company_reports__map',
            '_mr_key': 10,
            '_mr_value': None,
            '_mr_source_id': 20,
            '_mr_source_type': orgs,
            '_mr_version': 1,
            '_mr_deleted': False,
        },
        {
            'id': 2,
            'type': 'company_reports__map',
            '_mr_key': 10,
            '_mr_value': None,
            '_mr_source_id': 30,
            '_mr_source_type': reports,
            '_mr_version': 1,
            '_mr_deleted': False,
        },
    ]

Then reduce handler will receive::

    [1, 2]

Then it will fetch ``company_reports__map`` resources by given ids and then for
each ``company_reports__map`` resource it will fetch ``_mr_source_type`` using
``_mr_source_id`` and then do the mapping on that.

Reason why it is implemented this way is that you don't have to copy whole
resource content into the map resource, you just need the key.
