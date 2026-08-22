import logging
import json
import os
import weakref
import pandas as pd

# psycopg2, pandas_gbq and google.auth are imported inside the branch that uses
# them. They were module-level, which meant a DUCKDB run could not start without
# the Postgres client library and the whole GCP auth stack installed -- none of
# which it touches. Deferring them only moves the ImportError from import time to
# connect time, for the mode that actually needs the dependency.

# --- fork safety for DuckDB --------------------------------------------------
# An open DuckDB connection owns a background thread pool. fork() copies the
# mutexes but not the threads, so a child of a process that holds an open
# connection deadlocks -- on its FIRST query, before it can do anything about it,
# and even if it opens a brand new connection of its own. Measured: parent does
# any query and keeps the handle -> child hangs; parent closes the handle first
# -> child is fine. It is not the pandas conversion; plain fetchall() hangs too.
#
# This is not hypothetical here. validate.py builds a connection, calls
# get_n_patient_id() on it, and then forks a multiprocessing pool -- so every
# worker would hang, deterministically, not intermittently.
#
# So: close every live DuckDB connection immediately before any fork, and let it
# reopen lazily on the next query. Both sides of the fork stay correct and the
# common (unforked) case still reuses one connection.
_DUCKDB_LIVE = weakref.WeakSet()


def _duckdb_close_before_fork():
    for conn in list(_DUCKDB_LIVE):
        conn._duckdb_drop()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(before=_duckdb_close_before_fork)


class MFDatabaseConnection():
    def __init__(self, sqluser, sqlpass, dbname, host, db_mode, port=5432):
        # DUCKDB carries the database FILE PATH in `dbname`; sqluser, sqlpass,
        # host and port are all unused. Kept for the caller's signature.
        self._duckdb_path = dbname
        self._duckdb_pid = None
        self.db_mode = db_mode
        self.db_conn = self.connect_db(
            sqluser, sqlpass, dbname, host, db_mode, port
        )

    # database connection
    def connect_db(self, sqluser, sqlpass, dbname, host, db_mode, port=5432):
        if db_mode == 'POSTGRES':
            import psycopg2
            connection = psycopg2.connect(
                dbname=dbname,
                user=sqluser,
                password=sqlpass,
                host=host,
                port=port
            )
            connection.set_session(readonly=True)
        elif db_mode == 'BIGQUERY':
            import pandas_gbq as pdq
            import google.auth
            credentials, project = google.auth.default()
            pdq.context.credentials = credentials
            pdq.context.project = project
            connection = db_mode
        elif db_mode == 'DUCKDB':
            connection = self._duckdb_connect()
        else:
            connection = db_mode
        return connection

    def _duckdb_connect(self):
        import duckdb
        if not hasattr(duckdb, 'connect'):
            raise RuntimeError(
                '`import duckdb` found the repo\'s duckdb/ namespace package '
                'instead of the PyPI module -- run from outside the repo root'
            )
        # read_only is not a nicety. It mirrors POSTGRES's set_session(readonly),
        # and it is what lets validate.py's worker processes open the same file
        # at once -- DuckDB permits many concurrent readers but only one writer.
        conn = duckdb.connect(self._duckdb_path, read_only=True)
        self._duckdb_pid = os.getpid()
        _DUCKDB_LIVE.add(self)
        return conn

    def _duckdb_drop(self):
        """Release the handle without ending the object's usable life.

        Called before a fork, and by close(). The next read_query() reopens.
        """
        conn, self.db_conn, self._duckdb_pid = self.db_conn, None, None
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 -- already going away
                pass
        _DUCKDB_LIVE.discard(self)

    def _duckdb_handle(self):
        # Reconnect if dropped at a fork, or if we woke up in a different process
        # (a start method other than fork, or a handle that outlived its owner).
        if self.db_conn is None or self._duckdb_pid != os.getpid():
            self.db_conn = self._duckdb_connect()
        return self.db_conn

    def close(self):
        if self.db_mode == 'POSTGRES':
            self.db_conn.close()
        elif self.db_mode == 'DUCKDB':
            self._duckdb_drop()

    def read_query(self, query):
        if self.db_conn == 'BIGQUERY':
            import pandas_gbq as pdq
            df = pdq.read_gbq(query)
            if 'fhir' in df.columns:
                df['fhir'] = df['fhir'].apply(json.loads)
        elif self.db_mode == 'DUCKDB':
            df = self._duckdb_handle().execute(query).df()
            if 'fhir' in df.columns:
                # psycopg2 decodes a jsonb column to a dict for free, and every
                # caller here indexes it as one (`fhir['id']`). DuckDB's JSON
                # type arrives as str, exactly like BigQuery's, so it gets the
                # same treatment. Guarded, because a materialised table whose
                # fhir column is already STRUCT-typed would come back as a dict.
                df['fhir'] = df['fhir'].apply(
                    lambda v: json.loads(v) if isinstance(v, str) else v
                )
        else:
            df = pd.read_sql_query(query, self.db_conn)

        return df

    def get_table(self, schema, table):
        q_table = f"SELECT * FROM {schema}.{table};"
        df_table = self.read_query(q_table)
        return df_table

    def get_resources_by_pat(self, table_name, patient_id):
        if table_name == 'patient':
            id_field = 'id'
        else:
            id_field = 'patient_id'

        q_resource = f"""
            SELECT fhir FROM mimic_fhir.{table_name}
            WHERE {id_field} = '{patient_id}'
        """
        pd_resources = self.read_query(q_resource)
        resources = pd_resources.fhir.to_list()

        return resources

    def get_resource_by_id(self, profile, profile_id):
        q_resource = f"SELECT * FROM mimic_fhir.{profile} WHERE id='{profile_id}'"
        resource = self.read_query(q_resource)

        return resource.fhir[0]

    def get_n_patient_id(self, n_patient=0):
        if n_patient == 0:
            q_resource = f"SELECT * FROM mimic_fhir.patient"
        else:
            q_resource = f"SELECT * FROM mimic_fhir.patient LIMIT {n_patient}"
        resource = self.read_query(q_resource)
        patient_ids = [fhir['id'] for fhir in resource.fhir]

        return patient_ids

    def get_n_resources(self, table, n_limit=0):
        if n_limit == 0:
            q_resource = f"SELECT * FROM mimic_fhir.{table}"
        else:
            q_resource = f"SELECT * FROM mimic_fhir.{table} LIMIT {n_limit}"
        resource = self.read_query(q_resource)

        resources = []
        [resources.append(fhir) for fhir in resource.fhir]
        return resources

    # Function to find links between a patient and certain resources.
    # Allows for more complete testing if sending full bundle
    def get_pat_id_with_links(self, resource_list):
        q_resource = 'SELECT pat.fhir FROM mimic_fhir.patient pat'

        # Dynamically create query to find links between a patient and certain resources
        for idx, resource in enumerate(resource_list):
            q_resource = f"""{q_resource} 
                INNER JOIN mimic_fhir.{resource} t{idx}
                    ON pat.id = t{idx}.patient_id 
            """
        q_resource = f'{q_resource} LIMIT 20;'

        resource = self.read_query(q_resource)
        return resource.fhir[0]['id']
