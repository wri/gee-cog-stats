import io
import pathlib
import tempfile
import unittest

import download


class FakeBlob:
    def __init__(self, text):
        self.text = text

    def open(self, mode):
        return io.StringIO(self.text)


class FakeBucket:
    def __init__(self, objects):
        self.objects = objects

    def blob(self, name):
        return FakeBlob(self.objects[name])


class FakeStorageClient:
    def __init__(self, objects):
        self.objects = objects

    def bucket(self, name):
        return FakeBucket(self.objects[name])


class DownloadTest(unittest.TestCase):
    def test_sanitize_header_matches_bigquery_rules(self):
        header = 'Interval,Dataset,30 day active users,foo/bar baz'
        self.assertEqual(
            download.sanitize_header(header),
            'Start,End,Dataset,_30_day_active_users,foo_bar_baz',
        )

    def test_transform_source_file_splits_interval(self):
        objects = {
            'source': {
                'earthengine_stats_a.csv': (
                    'Interval,Dataset,30 day active users\n'
                    '2024-01-01/2024-01-31,projects/x,2\n'
                )
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / 'one.csv'
            rows = download.transform_source_file(
                FakeStorageClient(objects),
                'gs://source/earthengine_stats_a.csv',
                str(out),
            )

            self.assertEqual(rows, 1)
            self.assertEqual(
                out.read_text(),
                'Start,End,Dataset,_30_day_active_users\n'
                '2024-01-01,2024-01-31,projects/x,2\n',
            )

    def test_build_merge_query_uses_fact_key(self):
        refs = {
            'project_id': 'proj',
            'dataset_id': 'dataset',
            'target_table_id': 'facts',
        }
        query = download.build_merge_query(
            refs,
            'facts__staging',
            ['Start', 'End', 'Dataset', '_30_day_active_users'],
        )

        self.assertIn('MERGE `proj.dataset.facts` T', query)
        self.assertIn('USING `proj.dataset.facts__staging` S', query)
        self.assertIn('T.`Start` = S.`Start`', query)
        self.assertIn('T.`End` = S.`End`', query)
        self.assertIn('T.`Dataset` = S.`Dataset`', query)
        self.assertIn('`_30_day_active_users` = S.`_30_day_active_users`', query)

    def test_build_merge_query_requires_fact_key(self):
        refs = {
            'project_id': 'proj',
            'dataset_id': 'dataset',
            'target_table_id': 'facts',
        }

        with self.assertRaisesRegex(RuntimeError, 'Dataset'):
            download.build_merge_query(refs, 'facts__staging', ['Start', 'End'])


if __name__ == '__main__':
    unittest.main()
