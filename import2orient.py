#!/usr/bin/env python
from pyorient import OrientDB, STORAGE_TYPE_MEMORY, DB_TYPE_GRAPH
import yaml
from urllib.request import urlopen

db_name = 'omni4d'
model_url = (
    'https://raw.githubusercontent.com/omni4d/model/master/omni4d.core.yaml')

vertex_types = {
    'tuples': [
        'whole_part_tuple',
        'class_member_tuple',
        'ordinary_tuple'
    ],
    'classes': [
        'class_of_individuals',
        'class_of_events',
        'class_of_classes',
        'class_of_tuples'
    ],
    'events': [
        'event'
    ],
    'individuals': [
        'individual'
    ]
}


def model_from_url(url):
    file = urlopen(url)
    return yaml.load(file)


def create_vertex(sign, sign_type, client):
    client.command('create vertex %s set uuid = "%s"' % (sign_type, sign))


def create_vertices(model, client):
    edges = []
    for sign, attributes in model.items():
        create_vertex(sign, attributes['type'], client)

        if attributes['type'] in vertex_types['tuples']:
            for object, details in attributes['objects'].items():
                edges.append({
                    'from_sign': sign,
                    'to_sign': object,
                    'role': details['role']
                })
    return edges


def create_edge(from_sign, to_sign, role, client):
    client.command('create edge E from (select from V where uuid = "%s") to (select from V where uuid = "%s") set role="%s"' % (from_sign, to_sign, role))


def create_edges(edges, client):
    for edge in edges:
        create_edge(edge['from_sign'], edge['to_sign'], edge['role'], client)


def create_db(db_name, client):
    client.db_create(db_name, DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY)

    for sign_type, subtypes in vertex_types.items():
        for subtype in subtypes:
            client.command('create class %s extends V' % subtype)

if __name__ == '__main__':
    model = model_from_url(model_url)

    client = OrientDB('localhost', 2424)
    client.connect('admin', 'admin')

    if not client.db_exists(db_name, STORAGE_TYPE_MEMORY):
        create_db(db_name, client)

    client.db_open(db_name, 'admin', 'admin', DB_TYPE_GRAPH)
    edges = create_vertices(model, client)
    create_edges(edges, client)

    client.db_close(db_name)
