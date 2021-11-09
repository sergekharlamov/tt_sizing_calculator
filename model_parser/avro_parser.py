import json
import sys
from typing import List


class Field:
    def __init__(self, field_obj: dict):
        self._name = field_obj.get('name')
        self._type = field_obj.get('type')
        self._default = field_obj.get('default', None)

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return f'Field({{"name":{self._name}, "type":{self._type}, "default":{self._default}}})'


class Index:
    def __init__(self, index_name, index_fields: List[Field]):
        self._name = index_name
        self._parts = index_fields

    def __repr__(self):
        return f'Index(index_name={self._name}, index_fields={";".join(map(repr, self._parts))})'


class Space:
    def __init__(self, space_obj: dict):
        self._name = space_obj.get('name')
        self._type = space_obj.get('type')
        self._fields = [Field(field_obj) for field_obj in space_obj.get('fields')]
        self._indexes = self.set_indexes(space_obj.get('indexes', None))
        self._affinity = self.get_field(space_obj['affinity'][0]) if space_obj.get('affinity', None) is not None \
            else None
        self._logical_type = space_obj.get('logicalType', None)
        self._doc = space_obj.get('doc', None)
        self.is_space = True if len(self._indexes) else False
        self.relations = space_obj.get('relations', None)

    @property
    def name(self):
        return self._name

    def __str__(self):
        return self._name

    def __repr__(self):
        return f'''
Space({{
    "name": {self._name},
    "type": {self._type},
    "fields": {",".join([repr(field) for field in self._fields])},
    "indexes": {",".join([repr(index) for index in self._indexes])},
    "affinity": {self._affinity},
    "logical_type": {self._logical_type},
    "doc": {self._doc},
    "is_space": {self.is_space},
    "relations": {",".join([repr(relation) for relation in self.relations])}
}})
'''

    def get_field(self, field_name):
        for field in self._fields:
            if field_name == field.name:
                return field
        return None

    def set_indexes(self, index_obj):
        indexes = []
        if index_obj is None:
            return indexes

        for index in index_obj:
            if isinstance(index, str):
                indexes.append(Index(index, [self.get_field(index)]))
            elif isinstance(index, dict):
                indexes.append(
                    Index(
                            index.get('name'),
                            [self.get_field(index_part) for index_part in index.get('parts')]
                        )
                )
        return indexes


class Relation:
    def __init__(self, relation_obj, from_space, to_space):
        self._name = relation_obj.get('name')
        self._count = relation_obj.get('count')
        self._from_space = from_space
        self._from_fields = from_space.get_field(relation_obj.get('from_fields'))
        self._to_space = to_space
        self._to_fields = to_space.get_field(relation_obj.get('to_fields'))

    def __repr__(self):
        return f'''
Relation(
    from_space={str(self._from_space)},
    from_field={repr(self._from_fields)},
    to_space={str(self._to_space)}),
    to_field={repr(self._to_fields)},
    relation_obj={{
        "name": {self._name},
        "count": {self._count},
    }},
)'''


class Schema:
    def __init__(self, avro_filename: str):
        schema = None
        with open(avro_filename, 'r') as fh:
            schema = json.load(fh)

        self.spaces = [Space(space_obj) for space_obj in schema]
        self.relations = self.set_relations()

    def get_space(self, space_name):
        for space in self.spaces:
            if space.name == space_name:
                return space
        return None

    def set_relations(self):
        relations = []
        for space in self.spaces:
            if space.relations is None:
                continue
            space_relation = []
            for relation in space.relations:
                relation_space_to = self.get_space(relation.get('to'))
                space_relation.append(Relation(relation, space, relation_space_to))
            relations.extend(space_relation)
            space.relations = space_relation
        return relations

    def __repr__(self):
        return f'Schema(Spaces=[{",".join([repr(space) for space in self.spaces])}])'


if __name__ == '__main__':
    avro_filename = '/home/talkytitan/personalspace/tt_sizing_calculator/tdg_model.avsc'
    schema = Schema(avro_filename)
    print(schema)
