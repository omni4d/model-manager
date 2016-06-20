#!/usr/bin/env python
from pyorient import OrientDB, STORAGE_TYPE_MEMORY, DB_TYPE_GRAPH
import tqdm
import logging

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

logger = logging.getLogger(__name__)


def orient_id(sign, client):
    """
    Return the OrientDb Record ID (rid) for a given sign
    """
    records = client.command("select from V where uuid='%s'" % sign)
    if records:
        return records[0]._rid[1:]


def create_vertex(sign, sign_type, client):
    """
    Generate and execute the command to create a vertex
    """
    client.command('create vertex %s set uuid = "%s"' % (sign_type, sign))


def create_vertices(model, client):
    """
    From a model definition, create the vertexes and generate the list of
    edges required
    """
    edges = []
    for sign, attributes in tqdm.tqdm(model.items(), desc='Creating vertices',
                                      leave=True):
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
    """
    Create an edge if it doesn't already exists and the from and to signs do
    """
    # Check to see if the role exists as a subclass of E and create it if not
    query = "select from (select expand(classes) from metadata:schema) where name = '%s'" % role
    if not client.query(query):
        logger.info('Edge type %s does not exist. Creating it.', role)
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
    """
    From a list of edges, create each
    """
    for edge in tqdm.tqdm(edges, desc='Creating vertices', leave=True):
        create_edge(edge['from_sign'], edge['to_sign'], edge['role'], client)


def create_db(db_name, client):
    """
    Create a new database and populate it with the base types
    """
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
