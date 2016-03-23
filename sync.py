#!/usr/bin/env python
# coding: utf-8

import sys
import json
import time
import signal
import datetime
import ConfigParser

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

class Config:
    def __init__(self, fname):
        self.ini = ConfigParser.ConfigParser()
        self.fname = fname
        self.ini.read(fname)

    def parse_db_conn(self):
        db_host = self.ini.get("db", "host")
        db_port = self.ini.getint("db", "port")
        db_user = self.ini.get("db", "user")
        db_pass = self.ini.get("db", "pass")
        db_conn = {
            "host": db_host,
            "port": db_port,
            "user": db_user,
            "passwd": db_pass,
        }
        return db_conn

    def parse_schemas(self):
        return self.ini.get("slave", "schemas")
    def parse_server_id(self):
        return self.ini.getint("slave", "server_id")
    def parse_log_file(self):
        return self.ini.get("binlog", "log_file")
    def parse_log_pos(self):
        return self.ini.getint("binlog", "log_pos")
    def update_binlog(self, name, pos):
        self.ini.set("binlog", "log_file", name)
        self.ini.set("binlog", "log_pos", pos)
        with open(self.fname, 'wb') as configfile:
            self.ini.write(configfile)

def proc_es(action, event):
    pass

def proc_db(stream, conf):
    for binlogevent in stream:
        for row in binlogevent.rows:
            event = {"schema": binlogevent.schema, "table": binlogevent.table}

            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event = dict(event.items() + row["values"].items())
            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event = dict(event.items() + row["after_values"].items())
            elif isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event = dict(event.items() + row["values"].items())
            else:
                print "[WARN] do not support event type: ", binlogevent
                continue

            # proc not base types
            for key,val in event.items():
                if isinstance(val, datetime.datetime):
                    event[key] = "%s" % val

            try:
                print "[INFO] ", json.dumps(event)
                sys.stdout.flush()
            except Exception, e:
                print "[WARN] except: ", e

            action = event["action"]
            del event["action"]
            proc_es(action, event)
        fname, pos = stream.get_binlog()
        print "[INFO] binlog: ", fname, pos
        #conf.update_binlog(fname, pos)


def main(inifile):
    conf = Config(inifile)

    conn = conf.parse_db_conn()
    svrid = conf.parse_server_id()
    log_file = conf.parse_log_file()
    log_pos = conf.parse_log_pos()
    schemas = conf.parse_schemas()
    print conn
    print svrid, log_file, log_pos, schemas
    print 

    stream = BinLogStreamReader(
        connection_settings = conn,
        server_id = svrid,
        log_file = log_file,
        log_pos = log_pos,
        only_schemas = schemas,
        blocking = True,
        resume_stream = True,
        freeze_schema = True,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

    try:
        while True: 
            proc_db(stream, conf)
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print "[WARN] except, ", e
    finally:
        print "[INFO] close stream"
        stream.close()

if __name__ == "__main__":
    main("./sync.ini")

