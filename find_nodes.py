import psycopg2
from psycopg2 import sql


def database_connection(db_params):
    """
    Establish database connection to postgres
    """

    connection = psycopg2.connect("dbname=%s user=%s port=%s host=%s password=%s" (db_params['database_name'], db_params['user'], db_params['port'], db_params['host'], db_params['password']))

    return connection


def main(edges, nodes, edge_id_field='gid', node_id_field='gid', connection=None, connection_parameters=None, update_edges=False):
    """
    Calculate the closest node in a node dataset to the start and end of each edge in an edge dataset.
    """

    if connection is None:
        if connection_parameters is None:
            # return an error to the user
            return
        db_connection = database_connection(connection_parameters)

    # creat cursor to access database
    cursor = db_connection.cursor()

    # dictionary to store the edges and their closest nodes
    edge_set = {}

    # get edge features
    # if database connection passed, get data from database, if not
    # presume passed data is geojson

    # find the geometry column for the edges
    cursor.exeute(sql.SQL('SELECT f_geometry_column FROM geometry_columns WHERE f_table_name = {};').format(sql.Identifier(edges)))

    edge_geom_field = cursor.fetchone()[0]

    # get edges from database
    cursor.execut(sql.SQL('SELECT {}, ST_GeomAsText(ST_StartPoint({})) as start_node, ST_GeomAsText(ST_EndPoint({})) as end_node FROM {}').format(sql.Identifier(edge_id_field), sql.Identifier(edge_geom_field), sql.Identifier(edge_geom_field), sql.Identifier(edges)))

    edge_features = cursor.fetchall()

    # calculate the to and from nodes for the edges

    # find the geometry column for the nodes
    cursor.exeute(sql.SQL('SELECT f_geometry_column FROM geometry_columns WHERE f_table_name = {};').format(sql.Identifier(nodes)))

    node_geom_field = cursor.fetchone()[0]

    # loop through the edges returned from the database
    for feat in edge_features:

        # run for start node
        cursor.execute(sql.SQL('SELECT {} FROM {} ORDER BY {} <-> %s LIMIT 1;').format(sql.Identifier(node_id_field), sql.Identifier(nodes), sql.Identifier(node_geom_field)), [feat['start_node']])

        start_node_id = cursor.fetchone()[0]

        # run for end node
        cursor.execute(sql.SQL('SELECT {} FROM {} ORDER BY {} <-> %s LIMIT 1;').format(sql.Identifier(node_id_field), sql.Identifier(nodes), sql.Identifier(node_geom_field)), [feat['end_node']])

        end_node_id = cursor.fetchone()[0]

        # update the edges with the to and from ids if set as True
        if update_edges:

            cursor.execute(sql.SQL('UPDATE {} SET from_id=%s, to_id=%s WHERE gid=%s;').format(sql.Identifier(edges)), [start_node_id, end_node_id, feat[edge_id_field]])

        # store the information for use when inserting data in to the graph database
        edge_set[feat[edge_id_field]] = {'start_node':start_node_id, 'end_node':end_node_id}

    return edge_set