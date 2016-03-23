#!/usr/bin/env python
# coding: utf-8

import os
import re
import time
import json

defaults = "--single-transaction --skip-lock-tables --compact --skip-opt --quick --no-create-info" \
    "--master-data --skip-extended-insert"


# ignore_tables = ["soccerda.ndb_apply_status"]
def dump_mysql(conn, schemas, misc = defaults, ignore_tables = []):
    return "tmp/data_1458743695.sql"
    opts = "--host=%s --port=%s --user=%s --password=%s" % \
            (conn["host"], conn["port"], conn["user"], conn["passwd"])
    for table in ignore_tables:
        opts = "%s --ignore-table=%s" % (opts, table)

    opts = "%s %s" % (misc, opts)
    tfile = "tmp/data_%d.sql" % time.time()
    cmdstr = "mysqldump %s --databases %s >%s" % (opts, schemas, tfile)
    os.system(cmdstr)
    print cmdstr
    return tfile


# for table name
# pat_table = re.compile(r"CREATE TABLE `(.*)` \(\n")
def get_tables(conn, schemas):
    pat_key = re.compile(r" `(.*)` ")
    pat_item = re.compile(r"  `(.*)` (\w+)")

    tables = {}
    st = ""
    name = ""
    fsql = dump_mysql(conn, schemas, "-d")
    for line in file(fsql).readlines():
        if line[:12] == "CREATE TABLE":
            st = "table_begin"
            name = pat_key.search(line).groups(0)[0]
            tables [name] = {}
            #print name
        elif st == "table_begin":
            if line[:3] == "  `":
                res = pat_item.search(line).groups(0)
                item= res[0]; itype = res[1]
                tables[name][item] = {}
                tables[name][item]["type"] = itype
                tables[name][item]["val"] = ""
                #print item, itype
            else:
                st = "table_end"
    #print json.dumps(tables)
    return tables


if __name__ == "__main__":
    from sync import Config
    conf = Config("./sync.ini")
    conn = conf.parse_db_conn()
    schemas = conf.parse_schemas()

    #dump_mysql(conn, schemas)
    get_tables(conn, schemas)
