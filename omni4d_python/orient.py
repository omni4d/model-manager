#!/usr/bin/env python
from pyorient import OrientDB, STORAGE_TYPE_MEMORY, DB_TYPE_GRAPH

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

edge_types = ['whole', 'part', 'class', 'member']


def orient_id(sign, client):
    records = client.command("select from V where uuid='%s'" % sign)
    return records[0]._rid[1:]


def create_vertex(sign, sign_type, client):
    client.command('create vertex %s set uuid = "%s"' % (sign_type, sign))


def create_vertices(model, client):
    edges = []
    for sign, attributes in model.items():
        if not orient_id(sign, client):
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
    # Check to see if the role exists as a subclass of E and create it if not
    query = "select from (select expand(classes) from metadata:schema) where name = '%s'" % role
    if not client.query(query):
        client.command('create class %s extends E' % role)

    # Check that both the from_sign and to_sign exists as vertices
    orient_ids = {}
    for sign in [from_sign, to_sign]:
        rid = orient_id(sign, client)
        if rid:
            orient_ids[sign] = rid
        else:
            raise ValueError('Cannot find vertex for %s' % sign)

    # Check whether the edge already exists and create it if not
    query = "select * from E where out = %s and in = %s" % (orient_ids[from_sign], orient_ids[to_sign])
    edge = client.query(query)
    if not edge:
        query = 'create edge %s from %s to %s' % (role, orient_ids[from_sign], orient_ids[to_sign])
        client.command(query)


def create_edges(edges, client):
    for edge in edges:
        create_edge(edge['from_sign'], edge['to_sign'], edge['role'], client)


def create_db(db_name, client):
    client.db_create(db_name, DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY)

    for sign_type, subtypes in vertex_types.items():
        for subtype in subtypes:
            client.command('create class %s extends V' % subtype)

    for edge_type in edge_types:
        client.command('create class %s extends E' % edge_type)


def import_model(model, db_name, server, port, user, password):

    client = OrientDB(server, port)
    client.connect(user, password)

    if not client.db_exists(db_name, STORAGE_TYPE_MEMORY):
        create_db(db_name, client)

    client.db_open(db_name, user, password, DB_TYPE_GRAPH)
    edges = create_vertices(model, client)
    create_edges(edges, client)

    client.db_close(db_name)
