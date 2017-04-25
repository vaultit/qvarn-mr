Experimental Qvarn map/reduce service
#####################################

The idea is to create a service, that will listen for changes of all resource
types and then would route all changes throug all registered map/reduce
functions and will writes transformed data to other resource types.

This service should allow to join several resources into one and do
aggregations via reduce functions.

Nothing working has been implemented yet.


How to install
==============

Code was tested using python 3.4.5 and pip 9.0.1.

Create a virtualenv, activate it and run these commands::

  pip install -f vendor -r requirements.txt -e . 


How to run tests
================

Activate your virtualenv and run::

  py.test tests
