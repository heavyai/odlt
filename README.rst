OmniSci Data Library Transfer Utility
-------------------------------------

This package provides the ability to import OmniSci Library packages into OmniSci Core using pymapd.

Installation
------------

.. code-block::

    python setup.py install

Directory Setup
---------------

Both dashboards and datasets can be imported, and views can be created at the same time.

Successful import depends on the folder structure of the source data, set up as follows::

    /meaningfulname
        /tables
            /tablename1
                schema.sql
                /data
                    data1-1.csv.gz
                    data1-2.csv.gz
            /tablename2
                schema.sql
                /data
                    data2-1.csv.gz
                    data2-2.csv
        /views
            aview.sql
            anotherview.sql
            yetanotherview.sql
        /dashboards
            dashboard.json
            anotherdashboard.json
                  
Examples
--------
Importing
=========
Assuming the directory structure from above:

.. code-block::

    from odlt.importer import LibraryImport
    imp = LibraryImport()
    imp.connect()
    localpath = '/home/myuser/meaningfulname'
    corepath = '/opt/mapd/meaningfulname'
    imp.import_all(localpath=localpath, corepath=corepath)

ToDo
----

    - Write tests
    - AWS S3 Support
    - Google Cloud Storage Support
    - Support for exporting
    - Support fetching data from alternate sources
    - Incremental updates to table data

