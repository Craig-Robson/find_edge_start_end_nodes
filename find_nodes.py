import psycopg2
from psycopg2 import sql


def database_connection(db_params):
    """
    Establish database connection to postgres
    """

    connection = psycopg2.connect("dbname=%s user=%s port=%s host=%s password=%s" %(db_params['database_name'], db_params['user'], db_params['port'], db_params['host'], db_params['password']))

    connection.autocommit = True

    return connection


def main(edges, nodes, edge_id_field='gid', node_id_field='gid', cursor=None, connection=None, connection_parameters=None, update_edges=False):
    """
    Calculate the closest node in a node dataset to the start and end of each edge in an edge dataset.
    """

    if connection is None and cursor is None:
        if connection_parameters is None:
            # return an error to the user
            return
        db_connection = database_connection(connection_parameters)

    # create cursor to access database
    if cursor is None:
        cursor = db_connection.cursor()

    # set the names of the temp tables
    temp_edge_start_nodes = 'temp_edge_start_nodes'
    temp_edge_end_nodes = 'temp_edge_end_nodes'
    temp_edge_start_nodes_nearest = 'temp_edge_start_nodes_nearest'
    temp_edge_end_nodes_nearest = 'temp_edge_end_nodes_nearest'

    # delete the temp tables if left over from a failed run previously
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_start_nodes)))
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_end_nodes)))
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_start_nodes_nearest)))
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_end_nodes_nearest)))

    # dictionary to store the edges and their closest nodes
    edge_set = {}

    # get edge features
    # if database connection passed, get data from database, if not
    # I need a method to handle it

    # find the geometry column for the edges
    cursor.execute(sql.SQL('SELECT f_geometry_column FROM geometry_columns WHERE f_table_name = %s;'), [edges])

    edge_geom_field = cursor.fetchone()[0]

    # find the geometry column for the nodes
    cursor.execute(sql.SQL('SELECT f_geometry_column FROM geometry_columns WHERE f_table_name = %s;'), [nodes])

    node_geom_field = cursor.fetchone()[0]

    # create temp tables with geom in - edge start nodes
    cursor.execute(sql.SQL('SELECT gid, ST_StartPoint(ST_LineMerge({})) as geom INTO {} FROM {} ;').format(sql.Identifier(edge_geom_field), sql.Identifier(temp_edge_start_nodes), sql.Identifier(edges)), [])

    # create temp tables with geom in - edge end nodes
    cursor.execute(sql.SQL('SELECT gid, ST_EndPoint(ST_LineMerge({})) as geom INTO {} FROM {} ;').format(sql.Identifier(edge_geom_field), sql.Identifier(temp_edge_end_nodes), sql.Identifier(edges)), [])

    # add node id fields to the temp tables
    cursor.execute(sql.SQL('ALTER TABLE {} ADD node_id integer;').format(sql.SQL(temp_edge_start_nodes)))
    cursor.execute(sql.SQL('ALTER TABLE {} ADD node_id integer;').format(sql.SQL(temp_edge_end_nodes)))

    # run spatial search for start nodes
    cursor.execute(sql.SQL('SELECT {}.gid as edge_id, (SELECT {}.gid as node_id FROM {} ORDER BY {}.geom <#> {}.geom LIMIT 1) INTO {} FROM  {};').format(sql.SQL(temp_edge_start_nodes), sql.SQL(nodes), sql.SQL(nodes), sql.SQL(temp_edge_start_nodes), sql.SQL(nodes), sql.SQL(temp_edge_start_nodes_nearest), sql.SQL(temp_edge_start_nodes)))

    # run spatial search for end nodes
    cursor.execute(sql.SQL('SELECT {}.gid as edge_id, (SELECT {}.gid as node_id FROM {} ORDER BY {}.geom <#> {}.geom LIMIT 1) INTO {} FROM  {};').format(sql.SQL(temp_edge_end_nodes), sql.SQL(nodes), sql.SQL(nodes), sql.SQL(temp_edge_end_nodes), sql.SQL(nodes), sql.SQL(temp_edge_end_nodes_nearest), sql.SQL(temp_edge_end_nodes)))

    # create node id columns in original edge table if they don't exist
    cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS from_id integer;').format(sql.SQL(edges)))
    cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS to_id integer;').format(sql.SQL(edges)))

    # update node ids for start nodes
    cursor.execute(sql.SQL('UPDATE {} SET from_id = {}.node_id FROM {} WHERE {}.gid = {}.edge_id;').format(sql.SQL(edges), sql.SQL(temp_edge_start_nodes_nearest), sql.SQL(temp_edge_start_nodes_nearest), sql.SQL(edges), sql.SQL(temp_edge_start_nodes_nearest)))

    # update nodes ides for end nodes
    cursor.execute(sql.SQL('UPDATE {} SET to_id = {}.node_id FROM {} WHERE {}.gid = {}.edge_id;').format(sql.SQL(edges), sql.SQL(temp_edge_end_nodes_nearest), sql.SQL(temp_edge_end_nodes_nearest), sql.SQL(edges), sql.SQL(temp_edge_end_nodes_nearest)))

    # delete the temp tables on completion of run
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_start_nodes)))
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_end_nodes)))
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_start_nodes_nearest)))
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {};').format(sql.SQL(temp_edge_end_nodes_nearest)))


    return
