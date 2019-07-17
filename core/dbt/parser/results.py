from dataclasses import dataclass, field
from typing import TypeVar, Dict, Mapping, Union, List

from hologram import JsonSchemaMixin

from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.parsed import (
    ParsedNode, HasUniqueID, ParsedMacro, ParsedDocumentation, ParsedNodePatch,
    ParsedSourceDefinition, ParsedAnalysisNode, ParsedHookNode,
    ParsedModelNode, ParsedSeedNode, ParsedTestNode, ParsedSnapshotNode,
)
from dbt.contracts.util import Writable
from dbt.exceptions import (
    raise_duplicate_resource_name, raise_duplicate_patch_name,
    CompilationException
)
from dbt.node_types import NodeType


# Parsers can return anything as long as it's a unique ID
ParsedValueType = TypeVar('ParsedValueType', bound=HasUniqueID)


def _check_duplicates(value: HasUniqueID, src: Mapping[str, HasUniqueID]):
    if value.unique_id in src:
        raise_duplicate_resource_name(value, src[value.unique_id])


ManifestNodes = Union[
    ParsedAnalysisNode,
    ParsedHookNode,
    ParsedModelNode,
    ParsedSeedNode,
    ParsedTestNode,
    ParsedSnapshotNode,
]


@dataclass
class ParseResult(JsonSchemaMixin, Writable):
    nodes: Dict[str, ManifestNodes] = field(default_factory=dict)
    sources: Dict[str, ParsedSourceDefinition] = field(default_factory=dict)
    docs: Dict[str, ParsedDocumentation] = field(default_factory=dict)
    macros: Dict[str, ParsedMacro] = field(default_factory=dict)
    patches: Dict[str, ParsedNodePatch] = field(default_factory=dict)
    files: Dict[str, SourceFile] = field(default_factory=dict)
    disabled: List[ParsedNode] = field(default_factory=list)

    def get_file(self, source_file: SourceFile) -> SourceFile:
        key = source_file.search_key
        if key is None:
            return source_file
        if key not in self.files:
            self.files[key] = source_file
        return self.files[key]

    def add_source(
        self, source_file: SourceFile, node: ParsedSourceDefinition
    ):
        # nodes can't be overwritten!
        _check_duplicates(node, self.sources)
        self.sources[node.unique_id] = node
        self.get_file(source_file).sources.append(node.unique_id)

    def add_node(self, source_file: SourceFile, node: ParsedNode):
        # nodes can't be overwritten!
        _check_duplicates(node, self.nodes)
        self.nodes[node.unique_id] = node
        self.get_file(source_file).nodes.append(node.unique_id)

    def add_disabled(self, source_file: SourceFile, node: ParsedNode):
        self.disabled.append(node)
        self.get_file(source_file).nodes.append(node.unique_id)

    def add_macro(self, source_file: SourceFile, macro: ParsedMacro):
        # macros can be overwritten (should they be?)
        self.macros[macro.unique_id] = macro
        self.get_file(source_file).macros.append(macro.unique_id)

    def add_doc(self, source_file: SourceFile, doc: ParsedDocumentation):
        # Docs also can be overwritten (should they be?)
        self.docs[doc.unique_id] = doc
        self.get_file(source_file).docs.append(doc.unique_id)

    def add_patch(self, source_file: SourceFile, patch: ParsedNodePatch):
        # matches can't be overwritten
        if patch.name in self.patches:
            raise_duplicate_patch_name(patch.name, patch,
                                       self.patches[patch.name])
        self.patches[patch.name] = patch
        self.get_file(source_file).patches.append(patch.name)

    def sanitized_update(
        self, source_file: SourceFile, old_result: 'ParseResult',
    ) -> bool:
        """Perform a santized update. If the file can't be updated, invalidate
        it and return false.
        """
        old_file = old_result.get_file(source_file)
        if old_file.docs:
            return False

        for macro_id in old_file.macros:
            if macro_id not in old_result.macros:
                raise CompilationException(
                    'Expected to find "{}" in cached "manifest.macros" based '
                    'on cached file information: {}!'
                    .format(macro_id, old_file)
                )
            macro = old_result.macros[macro_id]
            self.add_macro(source_file, macro)

        for source_id in old_file.sources:
            if source_id not in old_result.sources:
                raise CompilationException(
                    'Expected to find "{}" in cached "manifest.sources" based '
                    'on cached file information: {}!'
                    .format(source_id, old_file)
                )
            source = old_result.sources[source_id]
            self.add_source(source_file, source)

        for node_id in old_file.nodes:
            if node_id not in old_result.nodes:
                raise CompilationException(
                    'Expected to find "{}" in cached "manifest.nodes" based '
                    'on cached file information: {}!'
                    .format(node_id, old_file)
                )
            # because we know this is how we _parsed_ the node, we can safely
            # assume if it's disabled it was done by the project or file, and
            # we can keep our old data
            node = old_result.nodes[node_id]
            if node.config.enabled:
                self.add_node(source_file, node)
            else:
                self.add_disabled(source_file, node)

        for name in old_file.patches:
            if name not in old_result.patches:
                raise CompilationException(
                    'Expected to find "{}" in cached "manifest.nodes" based '
                    'on cached file information: {}!'
                    .format(node_id, old_file)
                )
            patch = old_result.patches[name]
            self.add_patch(source_file, patch)

        return True

    def has_file(self, source_file: SourceFile) -> bool:
        key = source_file.search_key
        if key is None:
            return False
        if key not in self.files:
            return False
        my_checksum = self.files[key].checksum
        return my_checksum == source_file.checksum
