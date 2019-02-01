from unittest.mock import MagicMock, patch
from odlt import LibraryImport
import pymapd
import pytest

datalibrary = {
    'tables': {
        'footable': {'schema': '', 'data':''}
    },
    'dashboards': ['/faskpath/dashboard.json'],
    'views': ['/fakepath/views.sql']
}


@patch.object(LibraryImport, "_detect_source", lambda _: None)
@patch.object(LibraryImport, "readfile", lambda _, path: "test_content")
class TestLocalLibraryImport(object):
    @staticmethod
    def initialize_libraryimport():
        real = LibraryImport('/fakepath')
        real._conn = None
        real._source = 'local'
        real._calculate_files_info = MagicMock(return_value=datalibrary)
        return real
    
    @staticmethod
    def _add_default_connection_attributes(mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.execute.return_value = ['foo']
        client = mock_connection._client.return_value
        client.create_dashboard.retrun_value = None

    def test_calculate_files_info_called_once(self):
        real = self.__class__.initialize_libraryimport()
        assert real.datalibrary == datalibrary
        real.datalibrary, real.datalibrary
        assert real._calculate_files_info.call_count == 1
    
    def test_readfile_func(self):
        real = self.__class__.initialize_libraryimport()
        assert real.readfile('/fakepath') == 'test_content'
    
    def test_raise_exception_if_connection_not_established(self):
        real = self.__class__.initialize_libraryimport()
        with pytest.raises(ValueError):
            real._create_table('ffg')

    @patch('pymapd.connect')
    def test_pymapd_connection(self, mock_connection):
        self.__class__._add_default_connection_attributes(mock_connection)
        real = self.__class__.initialize_libraryimport()
        assert real.connect() == True
        real._create_table('ffg')

    @patch('pymapd.connect')
    def test_create_view(self, mock_connection):
        self.__class__._add_default_connection_attributes(mock_connection)
        real = self.__class__.initialize_libraryimport()
        assert real.connect() == True
        real._create_view('ffg')
    
    @patch.object(LibraryImport, "_get_file_or_obj_content", lambda x, y: "singleline")
    @patch('pymapd.connect')
    def test_import_dashboard_should_fail_for_invalid_dashboard_file(self, mock_connection):
        self.__class__._add_default_connection_attributes(mock_connection)
        real = self.__class__.initialize_libraryimport()
        real.connect()
        with pytest.raises(ValueError) as e:
            real._import_dashboard('/fakepath')
        assert str(e).endswith('Not a valid omnisci dashboard file format')

    @patch.object(LibraryImport, "_get_file_or_obj_content", lambda x, y: "dashname\nmetadata\nnot_a_json")
    @patch('pymapd.connect')
    def test_import_dashboard_should_fail_for_invalid_dashboard_definition(self, mock_connection):
        self.__class__._add_default_connection_attributes(mock_connection)
        real = self.__class__.initialize_libraryimport()
        real.connect()
        with pytest.raises(ValueError) as e:
            real._import_dashboard('/fakepath')
        assert str(e).endswith('Dashboard definition is not valid JSON')
    
    @patch.object(LibraryImport, "_get_file_or_obj_content", lambda x, y: 'dashname\nmetadata\n{"foo": "bar"}')
    @patch('pymapd.connect')
    def test_should_import_dashboard(self, mock_connection):
        self.__class__._add_default_connection_attributes(mock_connection)
        real = self.__class__.initialize_libraryimport()
        real.connect()
        real._import_dashboard('/fakepath')
