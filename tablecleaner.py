import argparse
from traceback import format_exc
import logging
import logging.handlers
import os.path
import sys
import os
import copy

import cassandra
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


import pprint

default_log = logging.getLogger('tablecleaner')
if os.environ.get('TABLECLEANER_SYSLOG', False):
    facility = logging.handlers.SysLogHandler.LOG_DAEMON
    syslog = logging.handlers.SysLogHandler(address='/dev/log', facility=facility)
    syslog.setFormatter(logging.Formatter('tablecleaner: %(message)s'))
    default_log.addHandler(syslog)
else:
    stderr = logging.StreamHandler()
    stderr.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    default_log.addHandler(stderr)

if os.environ.get('TDEBUG', False):
    default_log.setLevel(logging.DEBUG)
else:
    default_log.setLevel(logging.INFO)


default_log.info('Starting up')
cass_cluster = None
cass_session = None


def keys_for_table(keyspace_name, table_name):
    query = """SELECT column_name, type FROM system.schema_columns
                WHERE keyspace_name=%(keyspace)s AND columnfamily_name=%(table)s """

    params = {'keyspace': keyspace_name,
              'table': table_name }
    res = simple_execute(query, params)
    columns = []
    row_keys = []
    clustering_keys = []
    for r in res:
        if r.type == 'partition_key':
            row_keys.append(r.column_name)
        elif r.type == 'regular':
            columns.append(r.column_name)
        elif r.type == 'clustering_key':
            clustering_keys.append(r.column_name)
 
    return {'row_keys': row_keys,
            'clustering_keys': clustering_keys,
            'columns': columns}



def simple_execute(cql_query, cql_query_params={}, consistency_level=None, timeout=10.0, fetch_size=20):
    """ Simple wrapper around SimpleStatement to set a default CL=QUORUM, Timeout=10.0s, and fetch_size to use paginated results"""
    global cass_session

    if consistency_level is None:
        cl = consistency_level
    else:
        cl = cassandra.ConsistencyLevel.QUORUM 

    query = SimpleStatement(cql_query, cl, fetch_size=fetch_size)
    return cass_session.execute(query, parameters=cql_query_params, timeout=timeout)


def keys_iterator(keyspace, table_name):
    """
        Iterate over specified keyspace.table_name and return the unique partition keys
        SELECT DISTINCT will work for 2.0+, but will time out on any decently
        sized keyspace, and will fail for old versions of cassandar
        So, we'll try SELECT DISTINCT, and if it fails, we'll fall back to an
        older, safer iterator using tokens

    """
        
    keys = keys_for_table(keyspace, table_name)
    row_keys = keys['row_keys']
    clustering_keys = keys['clustering_keys']
    columns = keys['columns']


    try:
        key_str = ", ".join(str(r) for r in row_keys)
        key_where_str = """AND """.join((str(r) + "=%(" + str(r) + ")s ") for r in row_keys)

        keys_query = """SELECT DISTINCT %s  FROM %s """ % (key_str, table_name)
        keys_itr = simple_execute(keys_query)
        for k in keys_itr:
            yield k
    except:
        default_log.error("Exception while attempting to find all partition keys %s " % format_exc())
        default_log.info("SELECT DISTINCT failed. This is not necessarily a surprise, "
                         "it's likely that either rpc_timeout is exceeded or you are  "
                         "running an older version of cassandra where SELECT DISTINCT "
                         "is not supported, falling back to token iteration           ")


        try:
            starting_token_key_str = ", ".join(str(r) for r in row_keys)
            starting_token_where_str = """AND """.join((str(r) + "=%(" + str(r) + ")s ") for r in row_keys)
            query = """SELECT token(%s) as tablecleaner_token, %s FROM %s LIMIT 1""" % ( starting_token_key_str, 
                                                                 starting_token_key_str, 
                                                                 table_name )
            starting_token_row = simple_execute(query)
            if len(starting_token_row):
                default_log.debug(starting_token_row[0])
            else:
                return

            starting_token = starting_token_row[0].tablecleaner_token

            more_tokens = True
            while more_tokens:
                default_log.debug("Iterating through tokens, more tokens %s , current token %s " % (more_tokens, starting_token))
                query = """SELECT token(%s) as tablecleaner_token, %s FROM %s 
                            WHERE token(%s) > %s LIMIT 20 """ % ( starting_token_key_str, 
                                                                                  starting_token_key_str, 
                                                                                  table_name, 
                                                                                  starting_token_key_str,
                                                                                  starting_token)
                params = {'starting_token': starting_token}
                rows = simple_execute(query, params)
                if len(rows) < 1:
                    more_tokens = False
                else:
                    for r in rows:
                        yield r

                        starting_token = r.tablecleaner_token

        except:
            default_log.error("Error: %s " % format_exc())
            return
        
def main():
    global cass_session
    parser = argparse.ArgumentParser(description='tablecleaner is a script   '
        'that iterates over a CQL table and attempts to delete old entries.  '
        'While this could be accomplished using CQL TTLs, in some cases      '
        '(such as counters), TTLs are not available, and in others, the      '
        'application may want to lower TTLs and purge older data rather than '
        'waiting for the original TTL to be reached')
    parser.add_argument('--host',    help="Cassandra Endpoint")
    parser.add_argument('--keyspace',help="Keyspace")
    parser.add_argument('--table',   help="Table")
    parser.add_argument('--ttl',     help="Delete rows with TTL higher than [ttl]")
    parser.add_argument('--timestamp', 
                          help="Delete rows with WRITETIME lower than [timestamp]")
    parser.add_argument('--match_column_name', 
                          help="Selective delete: only rows where this column    "
                               "has value specified by --match_column_value      ")
    parser.add_argument('--match_column_value', 
                          help="Selective delete: only rows if --match_column_name"
                               "evauluates (as a string comparison) to this value")
    parser.add_argument('--test', action='store_true', 
                          help="Test only - print DELETE query but do not execute")

    args = parser.parse_args()
    
    if args.host is None:
        default_log.error('Invalid host')
        return -1
    else:
        host = args.host

    if args.keyspace is None:
        default_log.error('Invalid keyspace')
        return -1
    else:
        keyspace = args.keyspace

    if args.table is None:
        default_log.error('Invalid table')
        return -1
    else:
        table = args.table

    if args.test is True:
        really_delete = False
    else:
        really_delete = True

    ttl = None
    timestamp = None
    SYSTEM_KS = 'system'

    if args.ttl is None:
        default_log.debug("No TTL set")
    else:
        try:
            ttl = int(args.ttl)
        except:
            default_log.error("Invalid TTL: %s " % format_exc())
            return -1

    if args.timestamp is None:
        default_log.debug("No timestamp set")
    else:
        try:
            timestamp = int(args.timestamp)
        except:
            default_log.error("Invalid Timestamp: %s " % format_exc())
            return -1

    if timestamp is None and ttl is None:
        default_log.error("No TTL or Timestamp provided")
        return -1

    if args.match_column_name is not None and args.match_column_value is not None:
        selective_delete = True
        selective_delete_column_name = args.match_column_name
        selective_delete_column_value = args.match_column_value
        default_log.debug("Selective delete - only rows where column %s evaluates to %s " % ( args.match_column_name, args.match_column_value ))
    elif args.match_column_name is not None:
        default_log.error("--match_column_name requires --match_column_value")
        return -1
    elif args.match_column_value is not None:
        default_log.error("--match_column_value requires --match_column_name")
        return -1
    else:
        selective_delete = False


    try:
        cass_cluster = Cluster(host.split(','))
        cass_session = cass_cluster.connect(SYSTEM_KS)
    except:
        default_log.error("Cassandra connection error: %s " % format_exc())


    keys = keys_for_table(keyspace, table)
    row_keys = keys['row_keys']
    clustering_keys = keys['clustering_keys']
    columns = keys['columns']

    
    cass_session = cass_cluster.connect(keyspace)

    key_str = ", ".join(str(r) for r in row_keys)
    key_where_str = """AND """.join((str(r) + "=%(" + str(r) + ")s ") for r in row_keys)

    composite_keys = clustering_keys + row_keys
    composite_key_str = ", ".join(str(r) for r in composite_keys)
    composite_key_where_str = """AND """.join((str(r) + "=%(" + str(r) + ")s ") for r in composite_keys)

    for k in keys_iterator(keyspace, table):
        default_log.debug(k)
        partition_query = """SELECT %s, WRITETIME(%s) AS cleanwritetime, TTL(%s) AS cleanttl FROM %s WHERE %s """ % (composite_key_str, columns[0], columns[0], table, key_where_str)
        partition_params = dict() 
        for field in row_keys:
            partition_params[str(field)] = getattr(k, field)

        partition_results = simple_execute(partition_query, partition_params)
        for p_row in partition_results:
            delete_this_row = False

            if timestamp is None and ttl is not None:
                if p_row.cleanttl is not None and int(p_row.cleanttl) > int(ttl):
                    default_log.debug("Delete rows in this partition with TTL more than %s " % ttl)
                    delete_this_row = True
                else:
                    default_log.debug("Row TTL is safe: % <= %s" % ( p_row.cleanttl, ttl))
            elif ttl is None and timestamp is not None:
                if p_row.cleanwritetime is not None and int(p_row.cleanwritetime) < int(timestamp):
                    default_log.debug("Delete rows in this partition with timestamp less than %s " % timestamp)
                    delete_this_row = True
                else:
                    default_log.debug("Row timestamp is safe: %s >= %s " % ( p_row.cleanwritetime, timestamp))
            else: # Both timestamp and TTL
                if p_row.cleanwritetime is not None and int(p_row.cleanwritetime) < int(timestamp) and p_row.cleanttl is not None and int(p_row.cleanttl) > int(ttl):
                    default_log.debug("Delete rows in this partition with timestamp less than %s and ttl more than %s " % ( timestamp, ttl))
                    delete_this_row = True
                else:
                    default_log.debug("Row timestamp is safe or row TTL is safe - ttl %s <= %s and timestamp %s >= %s" % ( p_row.cleanttl, ttl, p_row.cleanwritetime, timestamp))


            if delete_this_row is True and selective_delete and str(getattr(p_row, selective_delete_column_name)) != str(selective_delete_column_value):
                delete_this_row = False
                default_log.debug("Row to be deleted does not match selective criteria - %s != %s " % ( str(getattr(p_row, selective_delete_column_name)), str(selective_delete_column_value)))

            if delete_this_row is True:
                delete_query = """DELETE FROM %s WHERE %s """ % ( table, composite_key_where_str )
                delete_query_params = copy.deepcopy(partition_params)

                # Add the corresponding clustering keys to the WHERE clause
                for ck in clustering_keys:
                    delete_query_params[str(ck)]= getattr(p_row, ck)

                default_log.debug("Issuing DELETE: Query %s , Params %s " % ( delete_query, delete_query_params))
                if really_delete:
                    simple_execute(delete_query, delete_query_params)


if __name__ == '__main__':
    sys.exit(main())
