"""

odlt.importer
=================================

This module contains the import classes for the OmniSci Data Library Transfer module.

Ex:

LocalImport
===========

from odlt import LibraryImport
imp = LibraryImport('/home/inspiron/Downloads/auto')
imp.connect()
imp.import_all()

S3 Import
=========

imp = LibraryImport('s3://some-s3-bucket/some-dataset-path', s3_access_key='xxxxxx', s3_secret_key='yyyyyyy')
imp.connect()
imp.import_all()
"""
import re
import os
import boto3
import pymapd
import glob
import base64
import logging
from odlt.utils import is_json, validate_connection
from mapd.ttypes import TCopyParams
from botocore.handlers import disable_signing

logging.basicConfig()
logger = logging.getLogger('odlt')


class LibraryImport(object):
    """
    public attributes:
      - source      : str  :  source where files get imported from. local or s3 
      - datalibrary : dict :  dictionary of caluclated file paths grouped by tables, dashboards, views, data
    """
    def __init__(self, conn=None, s3_access_key=None, s3_secret_key=None, s3_region=None):
        """
        :param str path: local or S3 datalibrary path
        :param pymapd.connection.Connection object conn: core instance connection
        """
        self._path = None
        self._conn = conn
        self._datalibrary = None
        self._errors = []
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key
        self._s3_region = s3_region
        self._source = None
        self._bucket = None
        self.copy_with_param_mapping = {
            'delimiter': 'delimiter',
            'null_str': 'nulls',
            'has_header': 'header',
            'quoted': 'quoted',
            'quote': 'quote',
            'escape': 'escape',
            'line_delim': 'line_delimiter',
            'array_delim': 'array_delimiter',
            'array_marker': 'array_marker',
            'threads': 'threads',
            's3_access_key': 's3_access_key',
            's3_secret_key': 's3_secret_key',
            's3_region': 's3_region',
            'geo_coords_encoding': 'geo_coords_encoding',
            'geo_coords_comp_param': 'geo_coords_comp_param', # doc missing
            'geo_coords_type': 'geo_coords_type',
            'geo_coords_srid': 'geo_coords_srid',
            'sanitize_column_names': 'sanitize_column_names', # doc missing
        }

    @property
    def source(self):
        return self._source
    @property
    def errors(self):
        return self._errors
    @property
    def datalibrary(self):
        """
        Always access by obj.dataset both internally and externally
        """
        if self._source and self._datalibrary is None:
            self._datalibrary = self._calculate_files_info()
        return self._datalibrary

    def _detect_source(self):
        # TODO: do path validations here
        if self._path.startswith('s3://'):
            self._source = 's3'
            self._bucket_name, *datapath = self._path.split('//')[1].split('/')
            if not datapath:
                raise ValueError('Not a valid S3 dataset path')
            self._datalibrary_path = '/'.join(datapath)
            if not self._bucket: self._initialize_s3_bucket()
        else:
            if not os.path.exists(self._path):
                raise ValueError('Dataset path {} doesnot exists'.format(self._path))
            self._source = 'local'

    def _initialize_s3_bucket(self):
        if self._s3_access_key and self._s3_secret_key:
            session = boto3.Session(
                aws_access_key_id=self._s3_access_key,
                aws_secret_access_key=self._s3_secret_key,
            )
            s3 = session.resource('s3')
        else:
            s3 = boto3.resource('s3')
            s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
        self._bucket = s3.Bucket(self._bucket_name)
    
    def _initialize_localpath(self, localpath):
        self._path = localpath
        self._detect_source()
        self._datalibrary = None

        return True

    def _calculate_files_info(self):
        # this should get called only after object initialization, at the very first access of obj.dataset property
        # TODO: folder structure validation
        data = {'tables': {}, 'dashboards': [], 'views': []}
        if self._source == 'local':
            for schema_path in glob.glob('{}/tables/*/schema.sql'.format(self._path)):
                *k, tblname, _ = schema_path.split('/')
                data_path, is_data_dir_exists = os.path.join(os.path.dirname(schema_path), 'data'), False
                if os.path.exists(data_path) and os.path.isdir(data_path):
                    is_data_dir_exists = True
                data['tables'][tblname] = {'schema': schema_path, 'data': data_path if is_data_dir_exists else ''}
            data['views'] = glob.glob('{}/views/*.sql'.format(self._path))
            data['dashboards'] = glob.glob('{}/dashboards/*.json'.format(self._path))

        elif self._source == 's3':
            tblschema_rgx = re.compile(r'/tables/(?P<table_name>[^/]+)/schema.sql$')
            view_rgx = re.compile(r'/views/.+$')
            dashboard_rgx = re.compile(r'/dashboards/.+')
            # TODO handle boto3 access denied exception
            for obj in self._bucket.objects.filter(Prefix=self._datalibrary_path):
                objname =  obj.key
                if not '/tables/' in objname and not '/views/' in objname and not '/dashboards/' in objname:
                    continue
                match = view_rgx.search(objname)
                if match:
                    data['views'].append(obj)
                    continue
                match = dashboard_rgx.search(objname)
                if match:
                    data['dashboards'].append(obj)
                    continue
                match = tblschema_rgx.search(objname)
                if match:
                    tblname = match.group('table_name')
                    if tblname in data['tables']:
                        data['tables'][tblname]['schema'] = obj
                    else:
                        data['tables'][tblname] = {'schema': obj, 'data': objname.replace('/schema.sql', '/data')}

        return data

    def connect(self, omnisciuser='mapd', omniscipass='HyperInteractive', dbname='mapd', port=9090, protocol='http', host='localhost'):
        """
        Connect to the OmniSci Core instance. 
        """
        self._conn = pymapd.connect(user=omnisciuser, password=omniscipass, host=host, dbname=dbname, port=port, protocol=protocol)
        return True

    def readfile(self, filepath):
        """
        Read contents from a file
        :param str filepath: path to the local file 
        """
        content = None
        if os.path.exists(filepath) and os.path.isfile(filepath):
            with open(filepath) as f:
                content = f.read()
        return content

    def read_s3obj(self, obj):
        """
        Read contents from s3 bucket object
        :param s3.ObjectSummary obj: s3 object
        """
        content = None
        if obj.__class__.__name__ == 's3.ObjectSummary':
            content = obj.get()['Body'].read().decode()
        return content

    def _get_file_or_obj_content(self, path_or_obj):
        """
        Get contents of a local file or s3 object
        :param s3.ObjectSummary(or)str path_or_obj : file path or s3 object
        """
        if self.source == 's3':
            return self.read_s3obj(path_or_obj)
        else:
            return self.readfile(path_or_obj)

    @validate_connection
    def _create_table(self, schemafile):
        """
        Create table from local schema file or s3 object
        :param s3.ObjectSummary(or)str schemafile : local file path or s3 object
        """
        cursor = self._conn.cursor()
        schema_qry = self._get_file_or_obj_content(schemafile)
        cursor.execute(schema_qry)

    @validate_connection
    def create_tables(self, localpath):
        """
        Create tables from schema queries
        """
        self._initialize_localpath(localpath)
        for tblname, tbldetails in self.datalibrary['tables'].items():
            self._create_table(tbldetails.get('schema'))


    @validate_connection
    def _create_view(self, viewfile):
        """
        Create view from a local file or s3 object
        :param s3.ObjectSummary(or)str viewfile : local file path or s3 object
        """
        cursor = self._conn.cursor()
        view_qry = self._get_file_or_obj_content(viewfile)
        cursor.execute(view_qry)

    @validate_connection
    def create_views(self, localpath):
        self._initialize_localpath(localpath)
        for view in self.datalibrary['views']:
            self._create_view(view)

    @validate_connection
    def _import_dashboard(self, dashfile):
        """
        Import dasboard from a local file or from s3 object
        :param s3.ObjectSummary(or)str dashfile : local file path or s3 object
        """
        content = self._get_file_or_obj_content(dashfile)
        content_lst = content.splitlines()
        if len(content_lst) != 3:
            raise ValueError('Not a valid omnisci dashboard file format')
        dashname, dashmetadata, dashdef = content_lst
        if not is_json(dashdef):
            raise ValueError('Dashboard definition is not valid JSON')
        dashdef64 = base64.b64encode(bytes(dashdef, 'utf-8')).decode()
        self._conn._client.create_dashboard(
            session=self._conn._session,
            dashboard_name=dashname,
            dashboard_state=dashdef64,
            image_hash='',
            dashboard_metadata=dashmetadata,
        )

    @validate_connection
    def import_dashboards(self, localpath):
        self._initialize_localpath(localpath)
        for dash in self.datalibrary['dashboards']:
            self._import_dashboard(dash)
        
        return True

    def _get_each_table_data_path(self,):
        """
        Generator func which yields table datapath on each iteration
        """
        for tablename, tbldetails in self.datalibrary['tables'].items():
            yield tablename, tbldetails['data']
    
    def get_withparams_from_copyparams(self, **copyparams):
        """
        Convert params passed by the user as with params ( https://www.omnisci.com/docs/latest/6_loading_data.html#csv )
        :copyparams kwargs
        :return dict
        """
        # combine array_begin and arrray_end kwargs
        array_begin, array_end = copyparams.get('array_begin'), copyparams.get('array_end')
        if array_begin and array_end:
            array_marker = array_begin + array_end
            copyparams['array_marker'] = array_marker
        
        with_clause_args = {}
        for key, val in copyparams.items():
            with_arg = self.copy_with_param_mapping.get(key)
            if with_arg:
                with_clause_args[with_arg] = val
        # combine initialized aws credentails
        if not with_clause_args.get('s3_access_key') and self._s3_access_key:
            with_clause_args['s3_access_key'] = self._s3_access_key
            
        if not with_clause_args.get('s3_secret_key') and self._s3_secret_key:
            with_clause_args['s3_secret_key'] = self._s3_secret_key
        
        if not with_clause_args.get('s3_region') and self._s3_region:
            with_clause_args['s3_region'] = self._s3_region

        return with_clause_args

    def load_data_using_copy_from_query(self, corepath=None, from_local=False, from_s3=False, **kwargs):
        """
        Load data using copy from query ( https://www.omnisci.com/docs/latest/6_loading_data.html#copy-from )
        """
        cursor = self._conn.cursor()
        for tblname, datapath in self._get_each_table_data_path():
            if not datapath: continue
            qry = None
            # do bulk data import from data folder
            if from_local:
                datapath = os.path.join(datapath, '*')
                if corepath:
                    datapath = datapath.replace(self._path, corepath)
                qry = "COPY {tblname} from '{datapath}'".format(tblname=tblname, datapath=datapath)
            elif from_s3:
                qry = "COPY from '{datapath}'".format(datapath=datapath)
            if qry:
                if kwargs:
                    withargs = self.get_withparams_from_copyparams(**kwargs)
                    formatted_withargs = ', '.join(["{key}='{val}'".format(key=key, val=val) for key, val in withargs.items()])
                    qry += " WITH ({})".format(formatted_withargs)
                cursor.execute(qry)

        return True

    def load_data_using_api(self, corepath=None, from_local=False, from_s3=False, **kwargs):
        """
        Load data using mapdcoreconn._client api
        :param bool from_local: True if the files are imported from local
        :param bool from_s3: True if the files are imported from S3
        """
        for tblname, datapath in self._get_each_table_data_path():
            if not datapath: continue
            if from_local:
                for df in glob.glob(os.path.join(datapath, '*')):
                    filename = df
                    if corepath:
                        filename = filename.replace(self._path, corepath)
                    self._conn._client.import_table(
                        session=self._conn._session,
                        table_name=tblname,
                        file_name=filename,
                        copy_params=TCopyParams(**kwargs)
                    )

            elif from_s3:
                pass
                # TODO: Add support for s3 data import
        
        return True

    @validate_connection
    def load_data(self, localpath, corepath=None, use_copy_from_qry=False, **kwargs):
        """
        Load data into the created tables.
        :param str corepath: (optional) The path to the root of the folder structure containing the data library, absolute or relative to the OmniSci Core server. If this is not passed then ``localpath`` is used.
        :param bool use_copy_from_qry: loads data using COPY FROM query

        :**kwargs: Optional keyword arguments to pass to the OmniSci Core load_table endpoint:
        :param str array_delim: A single-character string for the delimiter between input values contained within an array (default `,`)
        :param str array_begin: A single-character string indicating the start of an array (default `{`)
        :param str array_end: A single-character string indicating the end of an array (default `}`)
        :param str delimiter: A single-character string for the delimiter between input fields (default `,`)
        :param str escape: A single-character string for escaping quotes (default `'`)
        :param bool has_header: Either 'true' or 'false', indicating whether the input file has a header line in Line 1 that should be skipped (default `true`)
        :param str line_delim: A single-character string for terminating each line (default `true`)
        :param str null_str: A string pattern indicating that a field is NULL. (default: an empty string, `NA`, or `\\N`)
        :param str quote: A single-character string for quoting a field. (default: `"`)
        :param bool quoted: Either 'true' or 'false', indicating whether the input file contains quoted fields. (default: `true`)
        :param int threads: Number of threads for performing the data import. (default: Number of CPU cores on the system)
        :param str s3_access_key: S3 access key
        :param str s3_secret_key: S3 secret
        :param str s3_region: S3 region
        :param int geo_coords_encoding: tba
        :param int geo_coords_comp_param: tba
        :param int geo_coords_type: tba
        :param int geo_coords_srid: tba
        :param bool sanitize_column_names: tba
        TODO: Validation that each folder has a valid schema.sql file, skip if it does not
        """
        self._initialize_localpath(localpath)
        from_local = False
        from_s3 = False
        if self._source == 'local':
            from_local = True
        elif self._source == 's3':
            from_s3 = True

        if use_copy_from_qry:
            self.load_data_using_copy_from_query(corepath=corepath, from_local=from_local, from_s3=from_s3, **kwargs)
        else:
            self.load_data_using_api(corepath=corepath, from_local=from_local, from_s3=from_s3, **kwargs)
        
        return True

    @validate_connection
    def import_all(self, localpath, corepath=None, **kwargs):
        self.create_tables(localpath)
        self.create_views(localpath)
        self.import_dashboards(localpath)
        self.load_data(localpath, corepath=corepath, **kwargs)

        return True
