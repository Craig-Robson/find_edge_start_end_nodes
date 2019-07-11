import psycopg2
from psycopg2 import sql


def database_connection(db_params):
    """
    Establish database connection to postgres
    """

    connection = psycopg2.connect("dbname=%s user=%s port=%s host=%s password=%s" %(db_params['database_name'], db_params['user'], db_params['port'], db_params['host'], db_params['password']))
    connection.autocommit = True
    return connection


# def edge_analysis(node_id_field, egde_id_field, nodes, node_geom_field, feat, edges, db_params):
def edge_analysis(feat_):
    feat = feat_[0]
    params = feat_[1]

    node_id_field = params['node_id_field']
    edge_id_field = params['edge_id_field']
    nodes = params['nodes']
    node_geom_field = params['node_geom_field']
    edges = params['edges']
    db_params = params['connection_parameters']
    update_edges = params['update_edges']

    conn = database_connection(db_params)
    cursor = conn.cursor()

    print(feat)
    # run for start node
    cursor.execute(sql.SQL('SELECT {} FROM {} ORDER BY ST_Transform({}, 27700) <-> %s LIMIT 1;').format(sql.Identifier(
        node_id_field), sql.Identifier(nodes), sql.Identifier(node_geom_field)), [feat[1]])

    start_node_id = cursor.fetchone()[0]

    # run for end node
    cursor.execute(sql.SQL('SELECT {} FROM {} ORDER BY ST_Transform({}, 27700) <-> %s LIMIT 1;').format(sql.Identifier(
        node_id_field), sql.Identifier(nodes), sql.Identifier(node_geom_field)), [feat[2]])

    end_node_id = cursor.fetchone()[0]

    # update the edges with the to and from ids if set as True
    if update_edges:
        cursor.execute(sql.SQL('UPDATE {} SET from_id=%s, to_id=%s WHERE gid=%s;').format(sql.Identifier(
            edges)), [start_node_id, end_node_id, feat[0]])

    # store the information for use when inserting data in to the graph database
    # edge_set[feat[0]] = {'start_node':start_node_id, 'end_node':end_node_id}
    temp = {'start_node': start_node_id, 'end_node': end_node_id}

    cursor.close()
    conn.close()
    return temp


def main(edges, nodes, edge_id_field='gid', node_id_field='gid', connection=None, connection_parameters=None,
         update_edges=False):
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
    # I need a method to handle it

    # find the geometry column for the edges
    cursor.execute(sql.SQL('SELECT f_geometry_column FROM geometry_columns WHERE f_table_name = %s;'), [edges])

    edge_geom_field = cursor.fetchone()[0]

    # get edges from database - grab 50 at a time
    cursor.execute(sql.SQL(
        'SELECT {}, ST_AsEWKT(ST_Force_2D(ST_StartPoint(ST_SetSRID({},27700)))) as start_node, ST_AsEWKT(ST_Force_2D(ST_EndPoint(ST_SetSRID({}, 27700)))) as end_node FROM {} WHERE to_id is null LIMIT 10').format(
        sql.Identifier(edge_id_field), sql.Identifier(edge_geom_field), sql.Identifier(edge_geom_field),
        sql.Identifier(edges)))

    edge_features = cursor.fetchall()

    # calculate the to and from nodes for the edges

    # find the geometry column for the nodes
    cursor.execute(sql.SQL('SELECT f_geometry_column FROM geometry_columns WHERE f_table_name = %s;'), [nodes])

    node_geom_field = cursor.fetchone()[0]

    # loop through the edges returned from the database
    temp_ = {'node_id_field': node_id_field,
             'edge_id_field': edge_id_field,
             'nodes': nodes,
             'node_geom_field': node_geom_field,
             'edges': edges,
             'connection_parameters': connection_parameters,
             'update_edges': update_edges}

    feat_ = []
    for feat in edge_features:
        feat_.append([feat, temp_])

    with mp.Pool(5) as p:

        print(p.map(edge_analysis, feat_))
        # (node_id_field, egde_id_field, nodes, node_geom_field, feat, edges, connection_parameters)

    return edge_set