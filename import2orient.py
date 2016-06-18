from pyorient import OrientDB, STORAGE_TYPE_MEMORY, DB_TYPE_GRAPH
import yaml
from urllib.request import urlopen

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

file = urlopen(
    'https://raw.githubusercontent.com/omni4d/model/master/omni4d.core.yaml')
model = yaml.load(file)

db_name = 'omni4d'

client = OrientDB('localhost', 2424)
client.connect('admin', 'admin')
if not client.db_exists(db_name, STORAGE_TYPE_MEMORY):
    client.connect('admin', 'admin')
    client.db_create(db_name, DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY)

    for type, subtypes in vertex_types.items():
        for subtype in subtypes:
            client.command('create class %s extends V' % subtype)

    for edge_type in edge_types:
        client.command('create class %s extends E' % edge_type)

client.db_open(db_name, 'admin', 'admin')
for sign, attributes in model.items():
    client.command(
        'create vertex %s set uuid = "%s"' % (attributes['type'], sign))
    if attributes['type'] in vertex_types['tuples']:
        for object, details in attributes['objects'].items():
            client.command('create edge %s from (select from V where uuid = "%s") to (select from V where uuid = "%s")' % (details['role'], sign, object))

client.db_close(db_name)
