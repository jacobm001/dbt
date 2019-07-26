import unittest
from unittest import mock
from datetime import datetime

import os
import yaml

import dbt.flags
import dbt.parser
from dbt.parser import ModelParser, MacroParser, DataTestParser, \
    SchemaParser, ParserUtils

from dbt.parser.schema_test_builders import YamlBlock

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import Manifest, FilePath, SourceFile
from dbt.contracts.graph.parsed import ParsedModelNode, ParsedMacro, \
    ParsedNodePatch, ParsedSourceDefinition, NodeConfig, DependsOn, \
    ColumnInfo, ParsedTestNode, TestConfig
from dbt.contracts.graph.unparsed import FreshnessThreshold, Quoting, Time, \
    TimePeriod

from .utils import config_from_parts_or_dicts


def get_os_path(unix_path):
    return os.path.normpath(unix_path)


class BaseParserTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        dbt.flags.STRICT_MODE = True
        dbt.flags.WARN_ERROR = True

        self.maxDiff = None

        profile_data = {
            'target': 'test',
            'quoting': {},
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'host': 'localhost',
                    'schema': 'analytics',
                    'user': 'test',
                    'pass': 'test',
                    'dbname': 'test',
                    'port': 1,
                }
            }
        }

        root_project = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }

        self.root_project_config = config_from_parts_or_dicts(
            project=root_project,
            profile=profile_data,
            cli_vars='{"test_schema_name": "foo"}'
        )

        snowplow_project = {
            'name': 'snowplow',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
        }

        self.snowplow_project_config = config_from_parts_or_dicts(
            project=snowplow_project, profile=profile_data
        )

        self.all_projects = {
            'root': self.root_project_config,
            'snowplow': self.snowplow_project_config
        }
        self.patcher = mock.patch('dbt.context.parser.get_adapter')
        self.factory = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()


SINGLE_TABLE_SOURCE = '''
version: 2
sources:
    - name: my_source
      tables:
        - name: my_table
'''


class SchemaParserTest2(BaseParserTest):
    def setUp(self):
        super().setUp()

        self.parser = SchemaParser(
            project=self.root_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest
        )

    def yaml_block_for(self, test_yml: str, filename: str):
        root_dir = get_os_path('/usr/src/app')
        path = FilePath(
            searched_path='models',
            relative_path=filename,
            absolute_path=os.path.abspath(os.path.join(root_dir, filename))
        )
        return YamlBlock(data=yaml.safe_load(test_yml),
                         file=SourceFile.empty(path=path))

    def test__read_basic_source(self):
        block = self.yaml_block_for(SINGLE_TABLE_SOURCE, 'test_one.yml')
        self.assertEqual(len(list(self.parser.read_yaml_models(yaml=block))), 0)
        results = list(self.parser.read_yaml_sources(yaml=block))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source.name, 'my_source')
        self.assertEqual(results[0].table.name, 'my_table')
        self.assertEqual(len(results[0].tests), 0)
        self.assertEqual(len(results[0].columns), 0)

    def test__parse_basic_source(self):
        block = self.yaml_block_for(SINGLE_TABLE_SOURCE, 'test_one.yml')
        self.assertEqual(len(list(self.parser.parse_yaml_models)), 0)
        results = list(self.parser.parse_yaml_sources(yaml_block=block))
        tests = sorted(results, key=lambda n: n.name)
        parsed = sorted(self.parser.results.parsed.values(), key=lambda n: n.name)

        self.assertEqual(len(tests), 5)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(len(self.parser.results.patches), 0)




class SchemaParserTest(BaseParserTest):
    maxDiff = None

    def setUp(self):
        super().setUp()
        self.maxDiff = None

        self.macro_manifest = Manifest(macros={}, nodes={}, docs={},
                                       generated_at=datetime.utcnow(),
                                       disabled=[], files={})

        self.model_config = NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        })

        self.test_config = TestConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
            'severity': 'ERROR',
        })
        self.warn_test_config = self.test_config.replace(severity='WARN')

        self.disabled_config = {
            'enabled': False,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }

        self._expected_source = ParsedSourceDefinition(
            unique_id='source.root.my_source.my_table',
            name='my_table',
            description='my table description',
            source_name='my_source',
            source_description='my source description',
            loader='some_loader',
            package_name='root',
            root_path=os.getcwd(),
            path='test_one.yml',
            original_file_path='models/test_one.yml',
            columns={
                'id': ColumnInfo(name='id', description='user ID'),
            },
            docrefs=[],
            freshness=FreshnessThreshold(
                warn_after=Time(count=7, period=TimePeriod.hour),
                error_after=Time(count=20, period=TimePeriod.hour)
            ),
            loaded_at_field='something',
            database='test',
            schema='foo',
            identifier='bar',
            resource_type=NodeType.Source,
            quoting=Quoting(schema=True, identifier=False),
            fqn=['root', 'my_source', 'my_table']
        )

        self._expected_source_tests = [
            ParsedTestNode(
                alias='source_accepted_values_my_source_my_table_id__a__b',
                name='source_accepted_values_my_source_my_table_id__a__b',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.source_accepted_values_my_source_my_table_id__a__b',
                fqn=['root', 'schema_test',
                        'source_accepted_values_my_source_my_table_id__a__b'],
                package_name='root',
                original_file_path='models/test_one.yml',
                root_path=os.getcwd(),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on=DependsOn(),
                config=self.test_config,
                path=get_os_path(
                    'schema_test/source_accepted_values_my_source_my_table_id__a__b.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='ERROR') }}{{ test_accepted_values(model=source('my_source', 'my_table'), column_name='id', values=['a', 'b']) }}",
                description='',
                columns={},
                column_name='id',
            ),
            ParsedTestNode(
                alias='source_not_null_my_source_my_table_id',
                name='source_not_null_my_source_my_table_id',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.source_not_null_my_source_my_table_id',
                fqn=['root', 'schema_test', 'source_not_null_my_source_my_table_id'],
                package_name='root',
                root_path=os.getcwd(),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on=DependsOn(),
                config=self.test_config,
                original_file_path='models/test_one.yml',
                path=get_os_path('schema_test/source_not_null_my_source_my_table_id.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='ERROR') }}{{ test_not_null(model=source('my_source', 'my_table'), column_name='id') }}",
                description='',
                columns={},
                column_name='id',
            ),
            ParsedTestNode(
                alias='source_relationships_my_source_my_table_id__id__ref_model_two_',
                name='source_relationships_my_source_my_table_id__id__ref_model_two_',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.source_relationships_my_source_my_table_id__id__ref_model_two_', # noqa
                fqn=['root', 'schema_test',
                        'source_relationships_my_source_my_table_id__id__ref_model_two_'],
                package_name='root',
                original_file_path='models/test_one.yml',
                root_path=os.getcwd(),
                refs=[['model_two']],
                sources=[['my_source', 'my_table']],
                depends_on=DependsOn(),
                config=self.test_config,
                path=get_os_path('schema_test/source_relationships_my_source_my_table_id__id__ref_model_two_.sql'), # noqa
                tags=['schema'],
                raw_sql="{{ config(severity='ERROR') }}{{ test_relationships(model=source('my_source', 'my_table'), column_name='id', from='id', to=ref('model_two')) }}",
                description='',
                columns={},
                column_name='id',
            ),
            ParsedTestNode(
                alias='source_some_test_my_source_my_table_value',
                name='source_some_test_my_source_my_table_value',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.snowplow.source_some_test_my_source_my_table_value',
                fqn=['snowplow', 'schema_test', 'source_some_test_my_source_my_table_value'],
                package_name='snowplow',
                original_file_path='models/test_one.yml',
                root_path=os.getcwd(),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on=DependsOn(),
                config=self.warn_test_config,
                path=get_os_path('schema_test/source_some_test_my_source_my_table_value.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='WARN') }}{{ snowplow.test_some_test(model=source('my_source', 'my_table'), key='value') }}",
                description='',
                columns={},
            ),
            ParsedTestNode(
                alias='source_unique_my_source_my_table_id',
                name='source_unique_my_source_my_table_id',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.source_unique_my_source_my_table_id',
                fqn=['root', 'schema_test', 'source_unique_my_source_my_table_id'],
                package_name='root',
                root_path=os.getcwd(),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on=DependsOn(),
                config=self.warn_test_config,
                original_file_path='models/test_one.yml',
                path=get_os_path('schema_test/source_unique_my_source_my_table_id.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='WARN') }}{{ test_unique(model=source('my_source', 'my_table'), column_name='id') }}",
                description='',
                columns={},
                column_name='id',
            ),
        ]

        self._expected_model_tests = [
            ParsedTestNode(
                alias='accepted_values_model_one_id__a__b',
                name='accepted_values_model_one_id__a__b',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.accepted_values_model_one_id__a__b',
                fqn=['root', 'schema_test',
                        'accepted_values_model_one_id__a__b'],
                package_name='root',
                original_file_path='models/test_one.yml',
                root_path=os.getcwd(),
                refs=[['model_one']],
                sources=[],
                depends_on=DependsOn(),
                config=self.test_config,
                path=get_os_path(
                    'schema_test/accepted_values_model_one_id__a__b.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='ERROR') }}{{ test_accepted_values(model=ref('model_one'), column_name='id', values=['a', 'b']) }}",
                description='',
                columns={},
                column_name='id',
            ),
            ParsedTestNode(
                alias='not_null_model_one_id',
                name='not_null_model_one_id',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.not_null_model_one_id',
                fqn=['root', 'schema_test', 'not_null_model_one_id'],
                package_name='root',
                root_path=os.getcwd(),
                refs=[['model_one']],
                sources=[],
                depends_on=DependsOn(),
                config=self.test_config,
                original_file_path='models/test_one.yml',
                path=get_os_path('schema_test/not_null_model_one_id.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='ERROR') }}{{ test_not_null(model=ref('model_one'), column_name='id') }}",
                description='',
                columns={},
                column_name='id',
            ),
            ParsedTestNode(
                alias='relationships_model_one_id__id__ref_model_two_',
                name='relationships_model_one_id__id__ref_model_two_',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.relationships_model_one_id__id__ref_model_two_', # noqa
                fqn=['root', 'schema_test',
                        'relationships_model_one_id__id__ref_model_two_'],
                package_name='root',
                original_file_path='models/test_one.yml',
                root_path=os.getcwd(),
                refs=[['model_one'], ['model_two']],
                sources=[],
                depends_on=DependsOn(),
                config=self.test_config,
                path=get_os_path('schema_test/relationships_model_one_id__id__ref_model_two_.sql'), # noqa
                tags=['schema'],
                raw_sql="{{ config(severity='ERROR') }}{{ test_relationships(model=ref('model_one'), column_name='id', from='id', to=ref('model_two')) }}",
                description='',
                columns={},
                column_name='id',
            ),
            ParsedTestNode(
                alias='some_test_model_one_value',
                name='some_test_model_one_value',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.snowplow.some_test_model_one_value',
                fqn=['snowplow', 'schema_test', 'some_test_model_one_value'],
                package_name='snowplow',
                original_file_path='models/test_one.yml',
                root_path=os.getcwd(),
                refs=[['model_one']],
                sources=[],
                depends_on=DependsOn(),
                config=self.warn_test_config,
                path=get_os_path('schema_test/some_test_model_one_value.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='WARN') }}{{ snowplow.test_some_test(model=ref('model_one'), key='value') }}",
                description='',
                columns={},
            ),
            ParsedTestNode(
                alias='unique_model_one_id',
                name='unique_model_one_id',
                database='test',
                schema='analytics',
                resource_type=NodeType.Test,
                unique_id='test.root.unique_model_one_id',
                fqn=['root', 'schema_test', 'unique_model_one_id'],
                package_name='root',
                root_path=os.getcwd(),
                refs=[['model_one']],
                sources=[],
                depends_on=DependsOn(),
                config=self.warn_test_config,
                original_file_path='models/test_one.yml',
                path=get_os_path('schema_test/unique_model_one_id.sql'),
                tags=['schema'],
                raw_sql="{{ config(severity='WARN') }}{{ test_unique(model=ref('model_one'), column_name='id') }}",
                description='',
                columns={},
                column_name='id',
            ),
        ]

        self._expected_patch = ParsedNodePatch(
            name='model_one',
            description='blah blah',
            original_file_path='models/test_one.yml',
            columns={
                'id': ColumnInfo(name='id', description='user ID'),
            },
            docrefs=[],
        )

    def yaml_block_for(self, test_yml, filename):
        root_dir = get_os_path('/usr/src/app')
        path = FilePath(
            searched_path='models',
            relative_path=filename,
            absolute_path=os.path.abspath(os.path.join(root_dir, filename))
        )
        return YamlBlock(data=test_yml, file=SourceFile.empty(path=path))

    def test__read_source_schema_yaml(self):
        test_yml = yaml.safe_load('''
            version: 2
            sources:
                - name: my_source
                  loader: some_loader
                  description: my source description
                  quoting:
                    schema: True
                    identifier: True
                  freshness:
                    warn_after:
                        count: 10
                        period: hour
                    error_after:
                        count: 20
                        period: hour
                  loaded_at_field: something
                  schema: '{{ var("test_schema_name") }}'
                  tables:
                    - name: my_table
                      description: "my table description"
                      identifier: bar
                      freshness:
                        warn_after:
                            count: 7
                            period: hour
                      quoting:
                        identifier: False
                      columns:
                        - name: id
                          description: user ID
                          tests:
                            - unique:
                                severity: WARN
                            - not_null
                            - accepted_values:
                                values:
                                  - a
                                  - b
                            - relationships:
                                from: id
                                to: ref('model_two')
                      tests:
                        - snowplow.some_test:
                            key: value
                            severity: WARN
        ''')

        block = self.yaml_block_for(test_yml, 'test_one.yml')
        parser = SchemaParser(
            project=self.root_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest
        )

        self.assertEqual(len(list(parser.read_yaml_models(yaml=block))), 0)
        results = list(parser.read_yaml_sources(yaml=block))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source.name, 'my_source')
        self.assertEqual(results[0].table.name, 'my_table')
        self.assertEqual(results[0].tests, [{'snowplow.some_test': {'key': 'value', 'severity': 'WARN'}}])
        self.assertEqual(len(results[0].columns), 1)
        self.assertEqual(len(results[0].columns[0].tests), 4)

    def test__parse_source_schema_yml(self):
        test_yml = yaml.safe_load('''
            version: 2
            sources:
                - name: my_source
                  loader: some_loader
                  description: my source description
                  quoting:
                    schema: True
                    identifier: True
                  freshness:
                    warn_after:
                        count: 10
                        period: hour
                    error_after:
                        count: 20
                        period: hour
                  loaded_at_field: something
                  schema: '{{ var("test_schema_name") }}'
                  tables:
                    - name: my_table
                      description: "my table description"
                      identifier: bar
                      freshness:
                        warn_after:
                            count: 7
                            period: hour
                      quoting:
                        identifier: False
                      columns:
                        - name: id
                          description: user ID
                          tests:
                            - unique:
                                severity: WARN
                            - not_null
                            - accepted_values:
                                values:
                                  - a
                                  - b
                            - relationships:
                                from: id
                                to: ref('model_two')
                      tests:
                        - snowplow.some_test:
                            key: value
                            severity: WARN
        ''')

        block = self.yaml_block_for(test_yml, 'test_one.yml')
        parser = SchemaParser(
            project=self.root_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest
        )
        self.assertEqual(len(list(parser.parse_yaml_models(yaml_block=block))), 0)
        results = list(parser.parse_yaml_sources(yaml_block=block))

        tests = sorted(results, key=lambda n: n.name)
        parsed = sorted(parser.results.parsed.values(), key=lambda n: n.name)

        self.assertEqual(len(tests), 5)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(len(parser.results.patches), 0)

        for test, expected in zip(tests, self._expected_source_tests):
            self.assertEqual(test, expected)

        self.assertEqual(parsed[0], self._expected_source)

    # def test__model_schema(self):
    #     test_yml = yaml.safe_load('''
    #         version: 2
    #         models:
    #             - name: model_one
    #               description: blah blah
    #               columns:
    #                 - name: id
    #                   description: user ID
    #                   tests:
    #                     - unique:
    #                         severity: WARN
    #                     - not_null
    #                     - accepted_values:
    #                         values:
    #                           - a
    #                           - b
    #                     - relationships:
    #                         from: id
    #                         to: ref('model_two')
    #               tests:
    #                 - snowplow.some_test:
    #                     severity: WARN
    #                     key: value
    #     ''')
    #     parser = SchemaParser(
    #         self.root_project_config,
    #         self.all_projects,
    #         self.macro_manifest
    #     )
    #     results = list(parser.parse_schema(
    #         path='test_one.yml',
    #         test_yml=test_yml,
    #         package_name='root',
    #         root_dir=get_os_path('/usr/src/app')
    #     ))

    #     tests = sorted((node for t, node in results if t == 'test'),
    #                    key=lambda n: n.name)
    #     patches = sorted((node for t, node in results if t == 'patch'),
    #                      key=lambda n: n.name)
    #     sources = sorted((node for t, node in results if t == 'source'),
    #                      key=lambda n: n.name)
    #     self.assertEqual(len(tests), 5)
    #     self.assertEqual(len(patches), 1)
    #     self.assertEqual(len(sources), 0)
    #     self.assertEqual(len(results), 6)

    #     for test, expected in zip(tests, self._expected_model_tests):
    #         self.assertEqual(test, expected)


    #     self.assertEqual(patches[0], self._expected_patch)

    # def test__mixed_schema(self):
    #     test_yml = yaml.safe_load('''
    #         version: 2
    #         quoting:
    #           database: True
    #         models:
    #             - name: model_one
    #               description: blah blah
    #               columns:
    #                 - name: id
    #                   description: user ID
    #                   tests:
    #                     - unique:
    #                         severity: WARN
    #                     - not_null
    #                     - accepted_values:
    #                         values:
    #                           - a
    #                           - b
    #                     - relationships:
    #                         from: id
    #                         to: ref('model_two')
    #               tests:
    #                 - snowplow.some_test:
    #                     severity: WARN
    #                     key: value
    #         sources:
    #             - name: my_source
    #               loader: some_loader
    #               description: my source description
    #               quoting:
    #                 schema: True
    #                 identifier: True
    #               freshness:
    #                 warn_after:
    #                     count: 10
    #                     period: hour
    #                 error_after:
    #                     count: 20
    #                     period: hour
    #               loaded_at_field: something
    #               schema: '{{ var("test_schema_name") }}'
    #               tables:
    #                 - name: my_table
    #                   description: "my table description"
    #                   identifier: bar
    #                   freshness:
    #                     warn_after:
    #                         count: 7
    #                         period: hour
    #                   quoting:
    #                     identifier: False
    #                   columns:
    #                     - name: id
    #                       description: user ID
    #                       tests:
    #                         - unique:
    #                             severity: WARN
    #                         - not_null
    #                         - accepted_values:
    #                             values:
    #                               - a
    #                               - b
    #                         - relationships:
    #                             from: id
    #                             to: ref('model_two')
    #                   tests:
    #                     - snowplow.some_test:
    #                         severity: WARN
    #                         key: value
    #     ''')
    #     parser = SchemaParser(
    #         self.root_project_config,
    #         self.all_projects,
    #         self.macro_manifest
    #     )
    #     results = list(parser.parse_schema(
    #         path='test_one.yml',
    #         test_yml=test_yml,
    #         package_name='root',
    #         root_dir=get_os_path('/usr/src/app')
    #     ))

    #     tests = sorted((node for t, node in results if t == 'test'),
    #                    key=lambda n: n.name)
    #     patches = sorted((node for t, node in results if t == 'patch'),
    #                      key=lambda n: n.name)
    #     sources = sorted((node for t, node in results if t == 'source'),
    #                      key=lambda n: n.name)
    #     self.assertEqual(len(tests), 10)
    #     self.assertEqual(len(patches), 1)
    #     self.assertEqual(len(sources), 1)
    #     self.assertEqual(len(results), 12)

    #     expected_tests = self._expected_model_tests + self._expected_source_tests
    #     expected_tests.sort(key=lambda n: n.name)
    #     for test, expected in zip(tests, expected_tests):
    #         self.assertEqual(test, expected)

    #     self.assertEqual(patches[0], self._expected_patch)
    #     self.assertEqual(sources[0], self._expected_source)

    # def test__source_schema_invalid_test_strict(self):
    #     test_yml = yaml.safe_load('''
    #         version: 2
    #         sources:
    #             - name: my_source
    #               loader: some_loader
    #               description: my source description
    #               quoting:
    #                 schema: True
    #                 identifier: True
    #               freshness:
    #                 warn_after:
    #                     count: 10
    #                     period: hour
    #                 error_after:
    #                     count: 20
    #                     period: hour
    #               loaded_at_field: something
    #               schema: foo
    #               tables:
    #                 - name: my_table
    #                   description: "my table description"
    #                   identifier: bar
    #                   freshness:
    #                     warn_after:
    #                         count: 7
    #                         period: hour
    #                   quoting:
    #                     identifier: False
    #                   columns:
    #                     - name: id
    #                       description: user ID
    #                       tests:
    #                         - unique:
    #                             severity: WARN
    #                         - not_null
    #                         - accepted_values: # this test is invalid
    #                             - values:
    #                                 - a
    #                                 - b
    #                         - relationships:
    #                             from: id
    #                             to: ref('model_two')
    #                   tests:
    #                     - snowplow.some_test:
    #                         severity: WARN
    #                         key: value
    #     ''')
    #     parser = SchemaParser(
    #         self.root_project_config,
    #         self.all_projects,
    #         self.macro_manifest
    #     )
    #     root_dir = get_os_path('/usr/src/app')
    #     with self.assertRaises(dbt.exceptions.CompilationException):
    #         list(parser.parse_schema(
    #             path='test_one.yml',
    #             test_yml=test_yml,
    #             package_name='root',
    #             root_dir=root_dir
    #         ))

    # def test__source_schema_invalid_test_not_strict(self):
    #     dbt.flags.WARN_ERROR = False
    #     dbt.flags.STRICT_MODE = False
    #     test_yml = yaml.safe_load('''
    #         version: 2
    #         sources:
    #             - name: my_source
    #               loader: some_loader
    #               description: my source description
    #               quoting:
    #                 schema: True
    #                 identifier: True
    #               freshness:
    #                 warn_after:
    #                     count: 10
    #                     period: hour
    #                 error_after:
    #                     count: 20
    #                     period: hour
    #               loaded_at_field: something
    #               schema: foo
    #               tables:
    #                 - name: my_table
    #                   description: "my table description"
    #                   identifier: bar
    #                   freshness:
    #                     warn_after:
    #                         count: 7
    #                         period: hour
    #                   quoting:
    #                     identifier: False
    #                   columns:
    #                     - name: id
    #                       description: user ID
    #                       tests:
    #                         - unique:
    #                             severity: WARN
    #                         - not_null
    #                         - accepted_values: # this test is invalid
    #                             - values:
    #                                 - a
    #                                 - b
    #                         - relationships:
    #                             from: id
    #                             to: ref('model_two')
    #                   tests:
    #                     - snowplow.some_test:
    #                         severity: WARN
    #                         key: value
    #     ''')
    #     parser = SchemaParser(
    #         self.root_project_config,
    #         self.all_projects,
    #         self.macro_manifest
    #     )
    #     root_dir = get_os_path('/usr/src/app')
    #     results = list(parser.parse_schema(
    #         path='test_one.yml',
    #         test_yml=test_yml,
    #         package_name='root',
    #         root_dir=root_dir
    #     ))

    #     tests = sorted((node for t, node in results if t == 'test'),
    #                    key=lambda n: n.name)
    #     patches = sorted((node for t, node in results if t == 'patch'),
    #                      key=lambda n: n.name)
    #     sources = sorted((node for t, node in results if t == 'source'),
    #                      key=lambda n: n.name)
    #     self.assertEqual(len(tests), 4)
    #     self.assertEqual(len(patches), 0)
    #     self.assertEqual(len(sources), 1)
    #     self.assertEqual(len(results), 5)

    #     expected_tests = [x for x in self._expected_source_tests
    #                       if 'accepted_values' not in x.unique_id]
    #     for test, expected in zip(tests, expected_tests):
    #         self.assertEqual(test, expected)

    #     self.assertEqual(sources[0], self._expected_source)

    # @mock.patch.object(SchemaParser, 'find_schema_yml')
    # @mock.patch.object(dbt.parser.schemas, 'logger')
    # def test__schema_v2_as_v1(self, mock_logger, find_schema_yml):
    #     test_yml = yaml.safe_load(
    #         '{models: [{name: model_one, description: "blah blah", columns: ['
    #         '{name: id, description: "user ID", tests: [unique, not_null, '
    #         '{accepted_values: {values: ["a", "b"]}},'
    #         '{relationships: {from: id, to: ref(\'model_two\')}}]'
    #         '}], tests: [some_test: { key: value }]}]}'
    #     )
    #     find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
    #     root_project = {}
    #     all_projects = {}
    #     root_dir = '/some/path'
    #     relative_dirs = ['a', 'b']
    #     parser = dbt.parser.schemas.SchemaParser(root_project, all_projects, None)
    #     with self.assertRaises(dbt.exceptions.CompilationException) as cm:
    #         parser.load_and_parse(
    #             'test', root_dir, relative_dirs
    #         )
    #     self.assertIn('https://docs.getdbt.com/docs/schemayml-files',
    #                   str(cm.exception))

    # @mock.patch.object(SchemaParser, 'find_schema_yml')
    # @mock.patch.object(dbt.parser.schemas, 'logger')
    # def test__schema_v1_version_model(self, mock_logger, find_schema_yml):
    #     test_yml = yaml.safe_load(
    #         '{model_one: {constraints: {not_null: [id],'
    #         'unique: [id],'
    #         'accepted_values: [{field: id, values: ["a","b"]}],'
    #         'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
    #         '}}, version: {constraints: {not_null: [id]}}}'
    #     )
    #     find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
    #     root_project = {}
    #     all_projects = {}
    #     root_dir = '/some/path'
    #     relative_dirs = ['a', 'b']
    #     parser = dbt.parser.schemas.SchemaParser(root_project, all_projects, None)
    #     with self.assertRaises(dbt.exceptions.CompilationException) as cm:
    #         parser.load_and_parse(
    #             'test', root_dir, relative_dirs
    #         )
    #     self.assertIn('https://docs.getdbt.com/docs/schemayml-files',
    #                   str(cm.exception))

    # @mock.patch.object(SchemaParser, 'find_schema_yml')
    # @mock.patch.object(dbt.parser.schemas, 'logger')
    # def test__schema_v1_version_1(self, mock_logger, find_schema_yml):
    #     test_yml = yaml.safe_load(
    #         '{model_one: {constraints: {not_null: [id],'
    #         'unique: [id],'
    #         'accepted_values: [{field: id, values: ["a","b"]}],'
    #         'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
    #         '}}, version: 1}'
    #     )
    #     find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
    #     root_project = {}
    #     all_projects = {}
    #     root_dir = '/some/path'
    #     relative_dirs = ['a', 'b']
    #     parser = dbt.parser.schemas.SchemaParser(root_project, all_projects, None)
    #     with self.assertRaises(dbt.exceptions.CompilationException) as cm:
    #         parser.load_and_parse(
    #             'test', root_dir, relative_dirs
    #         )
    #     self.assertIn('https://docs.getdbt.com/docs/schemayml-files',
    #                   str(cm.exception))


# class ParserTest(BaseParserTest):
#     def _assert_parsed_sql_nodes(self, parse_result, parsed, disabled):
#         self.assertEqual(parse_result.parsed, parsed)
#         self.assertEqual(parse_result.disabled, disabled)


#     def find_input_by_name(self, models, name):
#         return next(
#             (model for model in models if model.get('name') == name),
#             {})

#     def setUp(self):
#         super().setUp()

#         self.macro_manifest = Manifest(macros={}, nodes={}, docs={},
#                                        generated_at=datetime.utcnow(), disabled=[])

#         self.model_config = NodeConfig.from_dict({
#             'enabled': True,
#             'materialized': 'view',
#             'persist_docs': {},
#             'post-hook': [],
#             'pre-hook': [],
#             'vars': {},
#             'quoting': {},
#             'column_types': {},
#             'tags': [],
#         })

#         self.test_config = TestConfig.from_dict({
#             'enabled': True,
#             'materialized': 'view',
#             'persist_docs': {},
#             'post-hook': [],
#             'pre-hook': [],
#             'vars': {},
#             'quoting': {},
#             'column_types': {},
#             'tags': [],
#             'severity': 'ERROR',
#         })

#         self.disabled_config = NodeConfig.from_dict({
#             'enabled': False,
#             'materialized': 'view',
#             'persist_docs': {},
#             'post-hook': [],
#             'pre-hook': [],
#             'vars': {},
#             'quoting': {},
#             'column_types': {},
#             'tags': [],
#         })

#     def test__single_model(self):
#         models = [{
#             'name': 'model_one',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'original_file_path': 'model_one.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'path': 'model_one.sql',
#             'raw_sql': ("select * from events"),
#         }]
#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.model_one': ParsedModelNode(
#                     alias='model_one',
#                     name='model_one',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.model_one',
#                     fqn=['root', 'model_one'],
#                     package_name='root',
#                     original_file_path='model_one.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='model_one.sql',
#                     raw_sql=self.find_input_by_name(
#                         models, 'model_one').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__single_model__nested_configuration(self):
#         models = [{
#             'name': 'model_one',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'original_file_path': 'nested/path/model_one.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'path': get_os_path('nested/path/model_one.sql'),
#             'raw_sql': ("select * from events"),
#         }]

#         self.root_project_config.models = {
#             'materialized': 'ephemeral',
#             'root': {
#                 'nested': {
#                     'path': {
#                         'materialized': 'ephemeral'
#                     }
#                 }
#             }
#         }

#         ephemeral_config = self.model_config.replace(materialized='ephemeral')

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )
#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.model_one': ParsedModelNode(
#                     alias='model_one',
#                     name='model_one',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.model_one',
#                     fqn=['root', 'nested', 'path', 'model_one'],
#                     package_name='root',
#                     original_file_path='nested/path/model_one.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=ephemeral_config,
#                     tags=[],
#                     path=get_os_path('nested/path/model_one.sql'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'model_one').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__empty_model(self):
#         models = [{
#             'name': 'model_one',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'model_one.sql',
#             'original_file_path': 'model_one.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': (" "),
#         }]

#         del self.all_projects['snowplow']
#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.model_one': ParsedModelNode(
#                     alias='model_one',
#                     name='model_one',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.model_one',
#                     fqn=['root', 'model_one'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='model_one.sql',
#                     original_file_path='model_one.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'model_one').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__simple_dependency(self):
#         models = [{
#             'name': 'base',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'base.sql',
#             'original_file_path': 'base.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': 'select * from events'
#         }, {
#             'name': 'events_tx',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'events_tx.sql',
#             'original_file_path': 'events_tx.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': "select * from {{ref('base')}}"
#         }]

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.base': ParsedModelNode(
#                     alias='base',
#                     name='base',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.base',
#                     fqn=['root', 'base'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='base.sql',
#                     original_file_path='base.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(models, 'base').get('raw_sql'),
#                     description='',
#                     columns={}

#                 ),
#                 'model.root.events_tx': ParsedModelNode(
#                     alias='events_tx',
#                     name='events_tx',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.events_tx',
#                     fqn=['root', 'events_tx'],
#                     package_name='root',
#                     refs=[['base']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='events_tx.sql',
#                     original_file_path='events_tx.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(models, 'events_tx').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__multiple_dependencies(self):
#         models = [{
#             'name': 'events',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'events.sql',
#             'original_file_path': 'events.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': 'select * from base.events',
#         }, {
#             'name': 'sessions',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'sessions.sql',
#             'original_file_path': 'sessions.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': 'select * from base.sessions',
#         }, {
#             'name': 'events_tx',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'events_tx.sql',
#             'original_file_path': 'events_tx.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("with events as (select * from {{ref('events')}}) "
#                         "select * from events"),
#         }, {
#             'name': 'sessions_tx',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'sessions_tx.sql',
#             'original_file_path': 'sessions_tx.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
#                         "select * from sessions"),
#         }, {
#             'name': 'multi',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'multi.sql',
#             'original_file_path': 'multi.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("with s as (select * from {{ref('sessions_tx')}}), "
#                         "e as (select * from {{ref('events_tx')}}) "
#                         "select * from e left join s on s.id = e.sid"),
#         }]

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.events': ParsedModelNode(
#                     alias='events',
#                     name='events',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.events',
#                     fqn=['root', 'events'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='events.sql',
#                     original_file_path='events.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'events').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.sessions': ParsedModelNode(
#                     alias='sessions',
#                     name='sessions',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.sessions',
#                     fqn=['root', 'sessions'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='sessions.sql',
#                     original_file_path='sessions.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'sessions').get('raw_sql'),
#                     description='',
#                     columns={},
#                 ),
#                 'model.root.events_tx': ParsedModelNode(
#                     alias='events_tx',
#                     name='events_tx',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.events_tx',
#                     fqn=['root', 'events_tx'],
#                     package_name='root',
#                     refs=[['events']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='events_tx.sql',
#                     original_file_path='events_tx.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'events_tx').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.sessions_tx': ParsedModelNode(
#                     alias='sessions_tx',
#                     name='sessions_tx',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.sessions_tx',
#                     fqn=['root', 'sessions_tx'],
#                     package_name='root',
#                     refs=[['sessions']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='sessions_tx.sql',
#                     original_file_path='sessions_tx.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'sessions_tx').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.multi': ParsedModelNode(
#                     alias='multi',
#                     name='multi',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.multi',
#                     fqn=['root', 'multi'],
#                     package_name='root',
#                     refs=[['sessions_tx'], ['events_tx']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='multi.sql',
#                     original_file_path='multi.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'multi').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#             },
#             []
#         )

#     def test__multiple_dependencies__packages(self):
#         models = [{
#             'name': 'events',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': 'events.sql',
#             'original_file_path': 'events.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': 'select * from base.events',
#         }, {
#             'name': 'sessions',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': 'sessions.sql',
#             'original_file_path': 'sessions.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': 'select * from base.sessions',
#         }, {
#             'name': 'events_tx',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': 'events_tx.sql',
#             'original_file_path': 'events_tx.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("with events as (select * from {{ref('events')}}) "
#                         "select * from events"),
#         }, {
#             'name': 'sessions_tx',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': 'sessions_tx.sql',
#             'original_file_path': 'sessions_tx.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
#                         "select * from sessions"),
#         }, {
#             'name': 'multi',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'multi.sql',
#             'original_file_path': 'multi.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("with s as "
#                         "(select * from {{ref('snowplow', 'sessions_tx')}}), "
#                         "e as "
#                         "(select * from {{ref('snowplow', 'events_tx')}}) "
#                         "select * from e left join s on s.id = e.sid"),
#         }]

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.snowplow.events': ParsedModelNode(
#                     alias='events',
#                     name='events',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.snowplow.events',
#                     fqn=['snowplow', 'events'],
#                     package_name='snowplow',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='events.sql',
#                     original_file_path='events.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'events').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.snowplow.sessions': ParsedModelNode(
#                     alias='sessions',
#                     name='sessions',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.snowplow.sessions',
#                     fqn=['snowplow', 'sessions'],
#                     package_name='snowplow',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='sessions.sql',
#                     original_file_path='sessions.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'sessions').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.snowplow.events_tx': ParsedModelNode(
#                     alias='events_tx',
#                     name='events_tx',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.snowplow.events_tx',
#                     fqn=['snowplow', 'events_tx'],
#                     package_name='snowplow',
#                     refs=[['events']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='events_tx.sql',
#                     original_file_path='events_tx.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'events_tx').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.snowplow.sessions_tx': ParsedModelNode(
#                     alias='sessions_tx',
#                     name='sessions_tx',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.snowplow.sessions_tx',
#                     fqn=['snowplow', 'sessions_tx'],
#                     package_name='snowplow',
#                     refs=[['sessions']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='sessions_tx.sql',
#                     original_file_path='sessions_tx.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'sessions_tx').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.multi': ParsedModelNode(
#                     alias='multi',
#                     name='multi',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.multi',
#                     fqn=['root', 'multi'],
#                     package_name='root',
#                     refs=[['snowplow', 'sessions_tx'],
#                           ['snowplow', 'events_tx']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='multi.sql',
#                     original_file_path='multi.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'multi').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#             },
#             []
#         )

#     def test__process_refs__packages(self):
#         nodes = {
#             'model.snowplow.events': ParsedModelNode(
#                 name='events',
#                 alias='events',
#                 database='test',
#                 schema='analytics',
#                 resource_type=NodeType.Model,
#                 unique_id='model.snowplow.events',
#                 fqn=['snowplow', 'events'],
#                 package_name='snowplow',
#                 refs=[],
#                 sources=[],
#                 depends_on=DependsOn(),
#                 config=self.disabled_config,
#                 tags=[],
#                 path='events.sql',
#                 original_file_path='events.sql',
#                 root_path=get_os_path('/usr/src/app'),
#                 raw_sql='does not matter',
#             ),
#             'model.root.events': ParsedModelNode(
#                 name='events',
#                 alias='events',
#                 database='test',
#                 schema='analytics',
#                 resource_type=NodeType.Model,
#                 unique_id='model.root.events',
#                 fqn=['root', 'events'],
#                 package_name='root',
#                 refs=[],
#                 sources=[],
#                 depends_on=DependsOn(),
#                 config=self.model_config,
#                 tags=[],
#                 path='events.sql',
#                 original_file_path='events.sql',
#                 root_path=get_os_path('/usr/src/app'),
#                 raw_sql='does not matter',
#             ),
#             'model.root.dep': ParsedModelNode(
#                 name='dep',
#                 alias='dep',
#                 database='test',
#                 schema='analytics',
#                 resource_type=NodeType.Model,
#                 unique_id='model.root.dep',
#                 fqn=['root', 'dep'],
#                 package_name='root',
#                 refs=[['events']],
#                 sources=[],
#                 depends_on=DependsOn(),
#                 config=self.model_config,
#                 tags=[],
#                 path='multi.sql',
#                 original_file_path='multi.sql',
#                 root_path=get_os_path('/usr/src/app'),
#                 raw_sql='does not matter',
#             ),
#         }

#         manifest = Manifest(
#             nodes=nodes,
#             macros={},
#             docs={},
#             generated_at=datetime.utcnow(),
#             disabled=[]
#         )

#         processed_manifest = ParserUtils.process_refs(manifest, 'root')
#         self.assertEqual(
#             processed_manifest.to_flat_graph()['nodes'],
#             {
#                 'model.snowplow.events': {
#                     'name': 'events',
#                     'alias': 'events',
#                     'database': 'test',
#                     'schema': 'analytics',
#                     'resource_type': 'model',
#                     'unique_id': 'model.snowplow.events',
#                     'fqn': ['snowplow', 'events'],
#                     'docrefs': [],
#                     'package_name': 'snowplow',
#                     'refs': [],
#                     'sources': [],
#                     'depends_on': {
#                         'nodes': [],
#                         'macros': []
#                     },
#                     'config': self.disabled_config.to_dict(),
#                     'tags': [],
#                     'path': 'events.sql',
#                     'original_file_path': 'events.sql',
#                     'root_path': get_os_path('/usr/src/app'),
#                     'raw_sql': 'does not matter',
#                     'columns': {},
#                     'description': '',
#                     'build_path': None,
#                     'patch_path': None,
#                 },
#                 'model.root.events': {
#                     'name': 'events',
#                     'alias': 'events',
#                     'database': 'test',
#                     'schema': 'analytics',
#                     'resource_type': 'model',
#                     'unique_id': 'model.root.events',
#                     'fqn': ['root', 'events'],
#                     'docrefs': [],
#                     'package_name': 'root',
#                     'refs': [],
#                     'sources': [],
#                     'depends_on': {
#                         'nodes': [],
#                         'macros': []
#                     },
#                     'config': self.model_config.to_dict(),
#                     'tags': [],
#                     'path': 'events.sql',
#                     'original_file_path': 'events.sql',
#                     'root_path': get_os_path('/usr/src/app'),
#                     'raw_sql': 'does not matter',
#                     'columns': {},
#                     'description': '',
#                     'build_path': None,
#                     'patch_path': None,
#                 },
#                 'model.root.dep': {
#                     'name': 'dep',
#                     'alias': 'dep',
#                     'database': 'test',
#                     'schema': 'analytics',
#                     'resource_type': 'model',
#                     'unique_id': 'model.root.dep',
#                     'fqn': ['root', 'dep'],
#                     'docrefs': [],
#                     'package_name': 'root',
#                     'refs': [['events']],
#                     'sources': [],
#                     'depends_on': {
#                         'nodes': ['model.root.events'],
#                         'macros': []
#                     },
#                     'config': self.model_config.to_dict(),
#                     'tags': [],
#                     'path': 'multi.sql',
#                     'original_file_path': 'multi.sql',
#                     'root_path': get_os_path('/usr/src/app'),
#                     'raw_sql': 'does not matter',
#                     'columns': {},
#                     'description': '',
#                     'build_path': None,
#                     'patch_path': None,
#                 }
#             }
#         )

#     def test__in_model_config(self):
#         models = [{
#             'name': 'model_one',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'model_one.sql',
#             'original_file_path': 'model_one.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("{{config({'materialized':'table'})}}"
#                         "select * from events"),
#         }]

#         self.model_config = self.model_config.replace(materialized='table')

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.model_one': ParsedModelNode(
#                     alias='model_one',
#                     name='model_one',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.model_one',
#                     fqn=['root', 'model_one'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     root_path=get_os_path('/usr/src/app'),
#                     path='model_one.sql',
#                     original_file_path='model_one.sql',
#                     raw_sql=self.find_input_by_name(
#                         models, 'model_one').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__root_project_config(self):
#         self.root_project_config.models = {
#             'materialized': 'ephemeral',
#             'root': {
#                 'view': {
#                     'materialized': 'view'
#                 }
#             }
#         }

#         models = [{
#             'name': 'table',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'table.sql',
#             'original_file_path': 'table.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("{{config({'materialized':'table'})}}"
#                         "select * from events"),
#         }, {
#             'name': 'ephemeral',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'ephemeral.sql',
#             'original_file_path': 'ephemeral.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }, {
#             'name': 'view',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'view.sql',
#             'original_file_path': 'view.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }]

#         self.model_config = self.model_config.replace(materialized='table')
#         ephemeral_config = self.model_config.replace(materialized='ephemeral')
#         view_config = self.model_config.replace(materialized='view')

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.table': ParsedModelNode(
#                     alias='table',
#                     name='table',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.table',
#                     fqn=['root', 'table'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path='table.sql',
#                     original_file_path='table.sql',
#                     config=self.model_config,
#                     tags=[],
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'table').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.ephemeral': ParsedModelNode(
#                     alias='ephemeral',
#                     name='ephemeral',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.ephemeral',
#                     fqn=['root', 'ephemeral'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path='ephemeral.sql',
#                     original_file_path='ephemeral.sql',
#                     config=ephemeral_config,
#                     tags=[],
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=self.find_input_by_name(
#                         models, 'ephemeral').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.view': ParsedModelNode(
#                     alias='view',
#                     name='view',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.view',
#                     fqn=['root', 'view'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path='view.sql',
#                     original_file_path='view.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     config=view_config,
#                     tags=[],
#                     raw_sql=self.find_input_by_name(
#                         models, 'ephemeral').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#             },
#             []
#         )

#     def test__other_project_config(self):
#         self.root_project_config.models = {
#             'materialized': 'ephemeral',
#             'root': {
#                 'view': {
#                     'materialized': 'view'
#                 }
#             },
#             'snowplow': {
#                 'enabled': False,
#                 'views': {
#                     'materialized': 'view',
#                     'multi_sort': {
#                         'enabled': True,
#                         'materialized': 'table'
#                     }
#                 }
#             }
#         }

#         self.snowplow_project_config.models = {
#             'snowplow': {
#                 'enabled': False,
#                 'views': {
#                     'materialized': 'table',
#                     'sort': 'timestamp',
#                     'multi_sort': {
#                         'sort': ['timestamp', 'id'],
#                     }
#                 }
#             }
#         }

#         models = [{
#             'name': 'table',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'table.sql',
#             'original_file_path': 'table.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("{{config({'materialized':'table'})}}"
#                         "select * from events"),
#         }, {
#             'name': 'ephemeral',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'ephemeral.sql',
#             'original_file_path': 'ephemeral.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }, {
#             'name': 'view',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'path': 'view.sql',
#             'original_file_path': 'view.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }, {
#             'name': 'disabled',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': 'disabled.sql',
#             'original_file_path': 'disabled.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }, {
#             'name': 'package',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': get_os_path('views/package.sql'),
#             'original_file_path': get_os_path('views/package.sql'),
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }, {
#             'name': 'multi_sort',
#             'resource_type': 'model',
#             'package_name': 'snowplow',
#             'path': get_os_path('views/multi_sort.sql'),
#             'original_file_path': get_os_path('views/multi_sort.sql'),
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': ("select * from events"),
#         }]

#         self.model_config = self.model_config.replace(materialized='table')

#         ephemeral_config = self.model_config.replace(
#             materialized='ephemeral'
#         )
#         view_config = self.model_config.replace(
#             materialized='view'
#         )
#         disabled_config = self.model_config.replace(
#             materialized='ephemeral',
#             enabled=False,
#         )
#         sort_config = self.model_config.replace(
#             materialized='view',
#             enabled=False,
#             sort='timestamp',
#         )
#         multi_sort_config = self.model_config.replace(
#             materialized='table',
#             sort=['timestamp', 'id'],
#         )

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             parsed={
#                 'model.root.table': ParsedModelNode(
#                     alias='table',
#                     name='table',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.table',
#                     fqn=['root', 'table'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path='table.sql',
#                     original_file_path='table.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     config=self.model_config,
#                     tags=[],
#                     raw_sql=self.find_input_by_name(
#                         models, 'table').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.ephemeral': ParsedModelNode(
#                     alias='ephemeral',
#                     name='ephemeral',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.ephemeral',
#                     fqn=['root', 'ephemeral'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path='ephemeral.sql',
#                     original_file_path='ephemeral.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     config=ephemeral_config,
#                     tags=[],
#                     raw_sql=self.find_input_by_name(
#                         models, 'ephemeral').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.root.view': ParsedModelNode(
#                     alias='view',
#                     name='view',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.view',
#                     fqn=['root', 'view'],
#                     package_name='root',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path='view.sql',
#                     original_file_path='view.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     config=view_config,
#                     tags=[],
#                     raw_sql=self.find_input_by_name(
#                         models, 'view').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#                 'model.snowplow.multi_sort': ParsedModelNode(
#                     alias='multi_sort',
#                     name='multi_sort',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.snowplow.multi_sort',
#                     fqn=['snowplow', 'views', 'multi_sort'],
#                     package_name='snowplow',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     path=get_os_path('views/multi_sort.sql'),
#                     original_file_path=get_os_path('views/multi_sort.sql'),
#                     root_path=get_os_path('/usr/src/app'),
#                     config=multi_sort_config,
#                     tags=[],
#                     raw_sql=self.find_input_by_name(
#                         models, 'multi_sort').get('raw_sql'),
#                     description='',
#                     columns={}
#                 ),
#             },
#             disabled=[
#                 ParsedModelNode(
#                     name='disabled',
#                     resource_type=NodeType.Model,
#                     package_name='snowplow',
#                     path='disabled.sql',
#                     original_file_path='disabled.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=("select * from events"),
#                     database='test',
#                     schema='analytics',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=disabled_config,
#                     tags=[],
#                     alias='disabled',
#                     unique_id='model.snowplow.disabled',
#                     fqn=['snowplow', 'disabled'],
#                     columns={}
#                 ),
#                 ParsedModelNode(
#                     name='package',
#                     resource_type=NodeType.Model,
#                     package_name='snowplow',
#                     path=get_os_path('views/package.sql'),
#                     original_file_path=get_os_path('views/package.sql'),
#                     root_path=get_os_path('/usr/src/app'),
#                     raw_sql=("select * from events"),
#                     database='test',
#                     schema='analytics',
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=sort_config,
#                     tags=[],
#                     alias='package',
#                     unique_id='model.snowplow.package',
#                     fqn=['snowplow', 'views', 'package'],
#                     columns={}
#                 )
#             ]
#         )

#     def test__simple_data_test(self):
#         tests = [{
#             'name': 'no_events',
#             'resource_type': 'test',
#             'package_name': 'root',
#             'path': 'no_events.sql',
#             'original_file_path': 'no_events.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'raw_sql': "select * from {{ref('base')}}"
#         }]

#         parser = DataTestParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(tests),
#             {
#                 'test.root.no_events': ParsedTestNode(
#                     alias='no_events',
#                     name='no_events',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Test,
#                     unique_id='test.root.no_events',
#                     fqn=['root', 'no_events'],
#                     package_name='root',
#                     refs=[['base']],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.test_config,
#                     path='no_events.sql',
#                     original_file_path='no_events.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     tags=[],
#                     raw_sql=self.find_input_by_name(
#                         tests, 'no_events').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__simple_macro(self):
#         macro_file_contents = """
# {% macro simple(a, b) %}
#   {{a}} + {{b}}
# {% endmacro %}
# """
#         parser = MacroParser(None, {})
#         result = parser.parse_macro_file(
#             macro_file_path='simple_macro.sql',
#             macro_file_contents=macro_file_contents,
#             root_path=get_os_path('/usr/src/app'),
#             package_name='root',
#             resource_type=NodeType.Macro)

#         self.assertTrue(callable(result['macro.root.simple'].generator))

#         self.assertEqual(
#             result,
#             {
#                 'macro.root.simple': ParsedMacro.from_dict({
#                     'name': 'simple',
#                     'resource_type': 'macro',
#                     'unique_id': 'macro.root.simple',
#                     'package_name': 'root',
#                     'depends_on': {
#                         'macros': []
#                     },
#                     'original_file_path': 'simple_macro.sql',
#                     'root_path': get_os_path('/usr/src/app'),
#                     'tags': [],
#                     'path': 'simple_macro.sql',
#                     'raw_sql': macro_file_contents,
#                 })
#             }
#         )

#     def test__simple_macro_used_in_model(self):
#         macro_file_contents = """
# {% macro simple(a, b) %}
#   {{a}} + {{b}}
# {% endmacro %}
# """
#         parser = MacroParser(None, {})
#         result = parser.parse_macro_file(
#             macro_file_path='simple_macro.sql',
#             macro_file_contents=macro_file_contents,
#             root_path=get_os_path('/usr/src/app'),
#             package_name='root',
#             resource_type=NodeType.Macro)

#         self.assertTrue(callable(result['macro.root.simple'].generator))

#         self.assertEqual(
#             result,
#             {
#                 'macro.root.simple': ParsedMacro.from_dict({
#                     'name': 'simple',
#                     'resource_type': 'macro',
#                     'unique_id': 'macro.root.simple',
#                     'package_name': 'root',
#                     'depends_on': {
#                         'macros': []
#                     },
#                     'original_file_path': 'simple_macro.sql',
#                     'root_path': get_os_path('/usr/src/app'),
#                     'tags': [],
#                     'path': 'simple_macro.sql',
#                     'raw_sql': macro_file_contents,
#                 }),
#             }
#         )

#         models = [{
#             'name': 'model_one',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'original_file_path': 'model_one.sql',
#             'root_path': get_os_path('/usr/src/app'),
#             'path': 'model_one.sql',
#             'raw_sql': ("select *, {{package.simple(1, 2)}} from events"),
#         }]

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.model_one': ParsedModelNode(
#                     alias='model_one',
#                     name='model_one',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.model_one',
#                     fqn=['root', 'model_one'],
#                     package_name='root',
#                     original_file_path='model_one.sql',
#                     root_path=get_os_path('/usr/src/app'),
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='model_one.sql',
#                     raw_sql=self.find_input_by_name(
#                         models, 'model_one').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )

#     def test__macro_no_explicit_project_used_in_model(self):
#         models = [{
#             'name': 'model_one',
#             'resource_type': 'model',
#             'package_name': 'root',
#             'root_path': get_os_path('/usr/src/app'),
#             'path': 'model_one.sql',
#             'original_file_path': 'model_one.sql',
#             'raw_sql': ("select *, {{ simple(1, 2) }} from events"),
#         }]

#         parser = ModelParser(
#             self.root_project_config,
#             self.all_projects,
#             self.macro_manifest
#         )

#         self._assert_parsed_sql_nodes(
#             parser.parse_sql_nodes(models),
#             {
#                 'model.root.model_one': ParsedModelNode(
#                     alias='model_one',
#                     name='model_one',
#                     database='test',
#                     schema='analytics',
#                     resource_type=NodeType.Model,
#                     unique_id='model.root.model_one',
#                     fqn=['root', 'model_one'],
#                     package_name='root',
#                     root_path=get_os_path('/usr/src/app'),
#                     refs=[],
#                     sources=[],
#                     depends_on=DependsOn(),
#                     config=self.model_config,
#                     tags=[],
#                     path='model_one.sql',
#                     original_file_path='model_one.sql',
#                     raw_sql=self.find_input_by_name(
#                         models, 'model_one').get('raw_sql'),
#                     description='',
#                     columns={}
#                 )
#             },
#             []
#         )
