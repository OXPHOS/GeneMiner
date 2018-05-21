# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Setup connector to PostgreSQL or Redshift with psycopg2

Author: Pan Deng

"""

import psycopg2
import __credential__


class LocalConnector:
    def __init__(self, psql=True):
        self.conn = None
        self.cur = None
        if psql:
            # Connect to PostgreSQL
            self.conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                                         user=__credential__.user_psql, password=__credential__.password_psql)
            self.cur = self.conn.cursor()
        else:
            # Connect to Redshift
            self.conn = psycopg2.connect(host=__credential__.host_redshift, dbname=__credential__.dbname_redshift,
                                         user=__credential__.user_redshift, password=__credential__.password_redshift,
                                         port=__credential__.port_redshift)
            self.cur = self.conn.cursor()

    def get_connection(self):
        return self.conn, self.cur

    def close_connection(self):
        self.conn.close()
        self.cur.close()