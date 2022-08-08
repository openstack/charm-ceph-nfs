import unittest
import ganesha


EXAMPLE_EXPORT = """## This export is managed by the CephNFS charm ##
EXPORT {
    # Each EXPORT must have a unique Export_Id.
    Export_Id = 1000;

    # The directory in the exported file system this export
    # is rooted on.
    Path = '/volumes/_nogroup/test_ganesha_share/e12a49ef-1b2b-40b3-ba6c';

    # FSAL, Ganesha's module component
    FSAL {
        # FSAL name
        Name = "Ceph";
        User_Id = "ganesha-test_ganesha_share";
        Secret_Access_Key = "AQCT9+9h4cwJOxAAue2fFvvGTWziUiR9koCHEw==";
    }

    # Path of export in the NFSv4 pseudo filesystem
    Pseudo = '/volumes/_nogroup/test_ganesha_share/e12a49ef-1b2b-40b3-ba6c';

    SecType = "sys";
    CLIENT {
        Access_Type = "rw";
        Clients = 0.0.0.0;
    }
    # User id squashing, one of None, Root, All
    Squash = "None";
}
"""


class ExportTest(unittest.TestCase):

    def test_parser(self):
        export = ganesha.Export.from_export(EXAMPLE_EXPORT)
        self.assertEqual(export.export_id, 1000)
        self.assertEqual(export.clients,
                         [{'Access_Type': 'rw', 'Clients': '0.0.0.0'}])
        self.assertEqual(export.name, 'test_ganesha_share')

    def test_add_client(self):
        export = ganesha.Export.from_export(EXAMPLE_EXPORT)
        export.add_client('10.0.0.0/8')
        self.assertEqual(
            export.clients,
            [{'Access_Type': 'rw', 'Clients': '0.0.0.0, 10.0.0.0/8'}])
        # adding again shouldn't duplicate export
        export.add_client('10.0.0.0/8')
        self.assertEqual(
            export.clients,
            [{'Access_Type': 'rw', 'Clients': '0.0.0.0, 10.0.0.0/8'}])

        export.add_client('192.168.0.0/16')
        self.assertEqual(
            export.clients,
            [{
                'Access_Type': 'rw',
                'Clients': '0.0.0.0, 10.0.0.0/8, 192.168.0.0/16'
            }])

    def test_remove_client(self):
        export = ganesha.Export.from_export(EXAMPLE_EXPORT)
        export.add_client('10.0.0.0/8')
        export.add_client('192.168.0.0/16')
        self.assertEqual(
            export.clients,
            [{
                'Access_Type': 'rw',
                'Clients': '0.0.0.0, 10.0.0.0/8, 192.168.0.0/16'
            }])
        export.remove_client('0.0.0.0')
        self.assertEqual(
            export.clients,
            [
                {'Access_Type': 'rw', 'Clients': '10.0.0.0/8, 192.168.0.0/16'},
            ])
