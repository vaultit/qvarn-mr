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
yet used by any real project. So theoretically it should work, but practically
it was not yet tested. So first integrators should expect rough edged.

The code is very basic and currently does not deal with huge amounts of data.
It just takes list of all available resource ids and process each id in a
synchronous way, one by one. This part should be improved, but if list of all
resource type ids fits into RAM, it should work fine.


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
section. Here is a simple example:

.. code-block:: python

    from qvarnmr.func import join, item


    handlers = {
        'company_reports__map': [
            {
                'source': 'orgs',
                'type': 'map',
                'map': item('id'),
            },
            {
                'source': 'reports',
                'type': 'map',
                'map': item('org'),
            },
        ],
        'company_reports': [
            {
                'source': 'company_reports__map',
                'type': 'reduce',
                'reduce': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        ]
    }

Handlers should be defined in an importable Python file.

Then you have to define new derived Qvarn resource types. Process how to define
new resource type is not yet clear, but you can follow `BOL-493`_, to know,
when the decision will be made.

.. _BOL-493: https://jira.tilaajavastuu.fi/browse/BOL-493

Once you have defined handlers and new resource types, you need to do initial
synchronisation by running this command::

  qvarnmr-resync path.to.handlers -c path/to/qvarnmr.cfg

``qvarnmr-resync`` will create Qvarn notification listeners and will run
initial map/reduce processing for each source resource type defined in the
``path.to.handlers`` configuration. If your source resource type have many
resources, this process can take a long time to complete and currently id does
not have a progress bar.

``-c path/to/qvarnmr.cfg`` is a Python configparser configuration file. Here is
example configuration file:

.. code-block:: ini

    [qvarn]
    base_url = https://qvarn-example.tld,
    client_id = test_client_id
    client_secret = verysecret
    verify_requests = no
    scope = scope1,scope2,scope3

    [gluu]
    base_url = https://gluu-example.tld

    [qvarnmr]
    instance = instance-name

In this configuration file you need to specify connection parameters for the
Qvarn and Gluu. Also you need to specify qvarnmr instance name. This name will
be used to know which notification handlers to use. There can be multiple
qvarnmr instances running, each processing different handlers. In order to
distinguish between these qvarnmr instances, instance name is used.

Probably it's a good idea to use project domain name as instance name. But
basically it can be anything, just make sure, that two instances does not have
the same name, because then each will steal notifications from one another.

Finally, when initial processing is done, you need to run qvarnmr daemon,
to process all changes continuously. You can do that by running following
command::

  qvarnmr-worker path.to.handlers -c path/to/qvarnmr.cfg -f

Here ``-f`` stands for *forever*. Other arguments are the same as for
``qvarnmr-resync``.

That's it.


How to define map/reduce handlers
=================================

Let's analyse the following example a bit further:

.. code-block:: python

    from qvarnmr.func import join, item


    handlers = {
        'company_reports__map': [
            {
                'source': 'orgs',
                'type': 'map',
                'map': item('id'),
            },
            {
                'source': 'reports',
                'type': 'map',
                'map': item('org'),
            },
        ],
        'company_reports': [
            {
                'source': 'company_reports__map',
                'type': 'reduce',
                'reduce': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        ]
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
        yield resource['id'], None

Each map function receives single argument, a resource. Each map function
should be a generator and should yield (**key**, **value**) tuples.

**Value** can be a ``None``, a scalar value or a dict. If **value** is a dict,
then it will be interpreted as a resource. If **value** is not a
dict, then it will be stored in ``_mr_value`` field in the target resource.

In cases, when you want more control you can decorate you map function with
``qvarnmr.func.mr_func`` decorator. For example:

.. code-block:: python

    from qvarnmr.func import mr_func

    @mr_func()
    def orgs_users(context, resource):
        for contract in context.qvarn.search('contracts', resource_id=resource['id']):
            person = contract.get_one('contract_parties', role='user')
            yield resource['id'], person['id']

With ``@mr_func()`` decorator you map function will get ``context`` argument.
Context is a namedtuples and has following fields:

- ``qvarn`` - ``QvarnApi`` instance for accessing Qvarn database.
- ``source_resource_type`` - source resource type.


How to define reduce function
-----------------------------

Reduce functions are very similar to the map functions, except reduce will get
generator of **resource ids** as a first argument. Note, that you will get
generator of just resource ids, not full resources.

For example, in order to get number of resources for each key yielded by map
function, you can simply pass ``qvarnmr.func.count`` as reduce function.
Handler definition will look like this:

.. code-block:: python

    from qvarnmr.func import count

    {
        'source': 'map',
        'type': 'reduce',
        'reduce': count,
    },

We can't use ``len`` here, because first argument is a generator, not a list.
That's why there is a ``count`` function, that will consume the generator and
returns number of generated items.

If you want to access whole resource, you have to do something like this:

.. code-block:: python

    @mr_func()
    def count_something_else(context, resources):
        return sum(
            resource['something_else']
            for resource in context.qvarn.get_multiple(context.source_resource_type, resources)
        )

To achieve same thing, you can also use ``map`` function for reduce handler,
like this:

.. code-block:: python

    from qvarnmr.func import value

    {
        'source': 'map',
        'type': 'reduce',
        'map': value('something_else'),
        'reduce': sum,
    },

Here, first argument for reduce function will be processed with
``value('something_else')``, which simply fetches the source resource and
returns value of ``something_else``.


How to define derived resource types
====================================

When defining new resource types for map/reduce results, you need to define
some special fields used by qvarnmr engine.

For map target resource type, these fields are required:

.. code-block:: python

    {
        'id': '',
        'type': '',
        'revision': '',
        '_mr_key': '',
        '_mr_value': '',
        '_mr_source_id': '',
        '_mr_source_type': '',
    }

Purpose of these fields:

- ``_mr_key`` - is a key, yielded by map function.
- ``_mr_value`` - if map functions yields a dict, this will be None, otherwise
  it will contain yielded value.
- ``_mr_source_id`` - resource id of a source resource type, this is needed to
  track resource updated and deletes.
- ``_mr_source_type`` - source resource type, this is needed to track resource
  updated and deletes.

For reduce target resource type, these fields are required:

.. code-block:: python

    {
        'id': '',
        'type': '',
        'revision': '',
        '_mr_key': '',
        '_mr_value': '',
    }

Purpose of these fields:

- ``_mr_key`` - is a key that represents group of values produced by map
  function with same key.
- ``_mr_value`` - same as with map, if reduce value is not a dict, then value
  will be assigned to this field.
