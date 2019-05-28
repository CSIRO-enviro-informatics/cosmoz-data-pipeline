# -*- coding: utf-8 -*-
# ★ UTF-8 ★
import pymssql
from contextlib import contextmanager
from _sql_db_config import consts as db_consts

class CosmozSQLConnection(object):

    def __init__(self, server=None, db=None, username=None, password=None):
        self.connection = None
        self.servername = server or db_consts['DB_HOST']
        self.dbname = db or db_consts['DB_NAME']
        self.username = username or db_consts['DB_USERNAME']
        self.password = password or db_consts['DB_PASSWORD']

    def __enter__(self):
        if self.connection is None:
            self.connect()
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()
            self.connection = None

    def get_connection(self, auto_connect=True):
        """
        :param auto_connect: bool
        :return: pymssql.Connection
        """
        if self.connection is None and auto_connect is True:
            self.connect()
        return self.connection

    @contextmanager
    def cursor(self, as_dict=False):
        """
        Returns a new pymssql db connection cursor.
        Automatically closes it too!
        :param as_dict: bool
        :return: pymssql.Cursor
        """
        assert self.connection is not None
        cursor = self.connection.cursor(as_dict=as_dict)
        yield cursor
        cursor.close()

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def connect(self, user=None, password=None):
        if user is None and self.username is None:
            raise RuntimeError("Username was not passed to constructor or to connect()")
        if password is None and self.password is None:
            raise RuntimeError("Password was not passed to constructor or to connect()")
        user = user or self.username
        password = password or self.password
        if user is not None and not str(user).upper().startswith("NEXUS"):
            user = "NEXUS\\{}".format(user)
        try:
            conn = pymssql.connect(server=self.servername, user=user, password=password,
                                   database=self.dbname)
        except Exception as e:
            raise e
        if conn is not None:
            self.connection = conn

