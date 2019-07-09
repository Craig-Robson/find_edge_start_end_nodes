# find nodes closest to the start and end of an edge
Find the nodes closest to the start and end of edges.

Using postgis functions within the PostgeSQL database finds for a set of edges the closest nodes to either end of an edge for a given set of nodes.

Run using the main function.

# Accepted variables
edges - the name of the edge table (string)  
nodes - the name of the node table (string)  
edge_id_field - the name of the field uniquely identifying edges (string) (default='gid')  
node_id_field - the name of the field uniquely identifying nodes (string) (default='gid')  
connection - an open database connection (psycopg2 connection object) (default=None)  
connection_parameters - a dictionary of parameters for a psycopg2 database connection (dictionary)  
update_edges - stroe the result of in the edge table in the database (boolean) (default=False)  

# Returns
dictionary of dicts with keys start_node and end_node
