#!/usr/bin/env python
from pyorient import OrientDB, STORAGE_TYPE_MEMORY, DB_TYPE_GRAPH
import tqdm
import logging

types = {
    'class': 'class_of_objects',
    'individual': 'individual',
    'event': 'event',
    'tuple': 'tuple'
}

edge_types = ['part', 'subpart', 'class', 'member']

logger = logging.getLogger(__name__)


def orient_id(sign, client):
    """
    Return the OrientDb Record ID (rid) for a given sign
    """
    records = client.command("select from V where uuid='%s'" % sign)
    if records:
        rid = records[0]._rid[1:]
        logger.debug('orient_id: returned %s as RID for %s' % (rid, sign))
    else:
        rid = None
    return rid


def create_vertex(sign, sign_type, client):
    """
    Generate and execute the command to create a vertex
    """
    rid = orient_id(sign, client)
    if not rid:
        command = 'create vertex %s set uuid = "%s"' % (sign_type, sign)
        logger.debug('create_vertex: Executing "%s"' % command)
        client.command(command)
        rid = orient_id(sign, client)
        logger.debug(
            'create_vertex: Vertex created with RID %s for %s' % (rid, sign))
    else:
        logger.debug(
            'create_vertex: Sign with RID %s already exists for %s' %
            (rid, sign))
    return rid


def create_vertices(model, client):
    """
    From a model definition, create the vertexes and generate the list of
    edges required
    """
    edges = []
    items = tqdm.tqdm(model.items(), desc='Creating vertices', leave=True)
    for sign, attributes in items:
        sign_type = types[attributes['type']]
        create_vertex(sign, sign_type, client)

        if attributes['type'] == types['tuple']:
            for object, details in attributes['objects'].items():
                edges.append({
                    'from_sign': sign,
                    'to_sign': object,
                    'role': details['role']
                })
    logger.debug('create_vertices: Found %i  edges to create' % len(edges))
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
            logger.error('ERROR: Cannot find vertex for %s' % sign)
            return

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


def create_db(db_name, server, port, user, password):
    """
    Create a new database and populate it with the base types
    """
    client = OrientDB(server, port)
    client.connect(user, password)

    client.db_create(db_name, DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY)

    for key, value in types.items():
        client.command('create class %s extends V' % value)

    for edge_type in edge_types:
        client.command('create class %s extends E' % edge_type)


def import_model(model, db_name, server, port, user, password):

    client = OrientDB(server, port)
    client.connect(user, password)

    client.db_open(db_name, user, password, DB_TYPE_GRAPH)
    edges = create_vertices(model, client)
    create_edges(edges, client)

    client.db_close(db_name)
