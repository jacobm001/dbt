from typing import Iterable

from hologram import ValidationError

from dbt.contracts.graph.parsed import (
    IntermediateSnapshotNode, ParsedSnapshotNode
)
from dbt.exceptions import (
    CompilationException, validator_error_message
)
from dbt.node_types import NodeType
from dbt.parser.base import SQLParser
from dbt.parser.search import (
    FilesystemSearcher, BlockContents, BlockSearcher, FileBlock
)


class SnapshotParser(
    SQLParser[IntermediateSnapshotNode, ParsedSnapshotNode]
):
    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.snapshot_paths, '.sql'
        )

    def parse_from_dict(self, dct, validate=True) -> IntermediateSnapshotNode:
        return IntermediateSnapshotNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Snapshot

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def set_snapshot_attributes(self, node):
        if node.config.target_database:
            node.database = node.config.target_database
        if node.config.target_schema:
            node.schema = node.config.target_schema

        return node

    def transform(self, node: IntermediateSnapshotNode) -> ParsedSnapshotNode:
        try:
            parsed_node = ParsedSnapshotNode.from_dict(node.to_dict())

            if parsed_node.config.target_database:
                node.database = parsed_node.config.target_database
            if parsed_node.config.target_schema:
                parsed_node.schema = parsed_node.config.target_schema

            return parsed_node

        except ValidationError as exc:
            raise CompilationException(validator_error_message(exc), node)

    def parse_file(self, file_block: FileBlock) -> None:
        blocks = BlockSearcher(
            source=[file_block],
            allowed_blocks={'snapshot'},
            source_tag_factory=BlockContents,
        )
        for block in blocks:
            self.parse_node(block)
