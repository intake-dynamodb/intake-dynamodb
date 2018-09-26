Quickstart
==========

``intake-dynamodb`` provides quick and easy access to stored in dynamodb files.

.. dynamodb: https://aws.amazon.com/dynamodb/

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c informaticslab intake-dynamodb

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Note that dynamodb sources do not yet support streaming from an Intake server.

Ad-hoc
~~~~~~

After installation, the function ``intake.open_dynamodb`` will become available. They can be used to open dynamodb datasets.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: dynamodb``.


Using a Catalog
~~~~~~~~~~~~~~~

