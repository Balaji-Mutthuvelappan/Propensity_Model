# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 11:44:18 2023

@author: hushond
"""
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 11:44:18 2023

@author: hushond
"""
from google.cloud import bigquery
import datetime
import math
import os
import pymssql
import pyodbc
import pandas as pd
import time


class BigQueryLoader:
    def __init__(self, project_id, credentials):
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.project_id = project_id
        self.credentials = credentials
        self.MSSQL_SERVER = 'AAO-LI-AHCRP002.thehutgroup.local' 

    def load_data_from_script(self, script_path):
        """
        Parameters
        ----------
        script_path : STRING
            The full file path to the script wanted to run in Big Query.
        Returns
        -------
        None.
        """
        try:
            print(os.getenv('USERNAME'),
                  f' Connection to Big Query was successful and is running a Big Query Script from {script_path}.')
            # Construct the job configuration with script content
            with open(script_path, 'r') as f:
                script_content = f.read()

            # Start the BigQuery job
            query_job = self.client.query(script_content)
            query_job.result()  # Wait for job to complete

            print(f'Data loaded successfully from {script_path} into BigQuery {self.client.project}')

        except Exception as e:
            print(f'Encountered the following exception {e}')

    def nero_to_bq(self, df, table_id, if_exists):
        """
        Parameters
        ----------
        df : Dataframe
            The dataset pulled from Nero to push to Big Query.
        if_exists : string
            there are 3 options to save into BigQuery tables:
        	    1. fail: If table exists raise pandas_gbq.gbq.TableCreationError
        	    2. replace: If table exists, drop it, recreate it, and insert data
        	    3. append: If table exists, insert data. Create if does not exist
        Returns
        -------
        None.
        """
        try:
            print(os.getenv('USERNAME'), 'Connection to Big Query was successful.')
            df.to_gbq(destination_table=table_id, project_id=self.project_id, if_exists=if_exists,
                      credentials=self.credentials)
            print(f'\nData import into Big Query {self.project_id}{table_id} the WRITE option was {if_exists}.')
        except Exception as e:
            print(f'Encountered the following exception {e}')

    def bq_to_nero(self, bq_sql, database_name, schema_table_name, if_exist_method):
        """
        Parameters
        ----------
        sql : STRING
            The Big Query script you to pull from Big Query and push to Nero.
        project_id : STRING
            The project the data is pulled from i.e.'agile-bonbon-662'.
        Returns
        -------
        None.
        """
        try:
            sql_query = bq_sql
            # Use pandas gbp package to pull from Big Query
            print(os.getenv('USERNAME'), 'Connection to Big Query was successful.')
            results_df = pd.read_gbq(query=sql_query, project_id=self.project_id, index_col=None,
                                     credentials=self.credentials, dialect='standard')
            print('Data sucessfully pulled from Big Query.')
            # Import Big Query data into sequel 
            self.fast_import_df_to_sql(self.project_id, self.credentials, results_df, database_name, schema_table_name,
                                       if_exists=if_exist_method, chunk_rows=50000)

        except Exception as e:
            print(f'Encountered the following exception {e}')

    def exec_bq_query(self, bq_sql):
        """
        Parameters
        ----------
        bq_sql : STRING
            The Big Query script you to pull from Big Query and push to Nero.
        Returns
        -------
        Big Query output from query supplied.
        """
        try:
            sql_query = bq_sql
            # Use pandas gbp package to pull from Big Query
            print(os.getenv('USERNAME'), 'Connection to Big Query was successful.')
            results_df = pd.read_gbq(query=sql_query, project_id=self.project_id, index_col=None,
                                     credentials=self.credentials, dialect='standard')
            print('Data successfully pulled from Big Query.')
            return results_df

        except Exception as e:
            print(f'Encountered the following exception {e}')

    def exec_query(self, sql, read=True, max_retries=10, waiting_time=15):
        """
        Parameters
        ----------
        sql : STRING
            Sequel query to be executed.
        read : 
            bool - if False then commit transaction
        Returns
        -------
        None.
        """
        for _ in range(max_retries):
            try:
                connection = pymssql.connect(server=self.MSSQL_SERVER)
                # print(connection)
                cursor = connection.cursor()
                break
            except Exception as e:  # to capture pymssql error
                print(f'Encountered the following exception {e}')
                print(f'Will be sleeping for {waiting_time}"')
                time.sleep(waiting_time)
                continue  # No need to rollback the transaction as the try-except happens during the connection creation

        try:
            begin_time = datetime.datetime.now()
            cursor = connection.cursor()
            print(os.getenv('USERNAME'), 'is running a MSSQL query.')
            cursor.execute(sql)
            if read:
                column_names = [i[0] for i in cursor.description]
                rows = cursor.fetchall()
                d = pd.DataFrame(data=rows, columns=column_names)
                query_duration = datetime.datetime.now() - begin_time
                print(f'Query complete after {query_duration}. Dataset with dimensions {d.shape} will be returned')
                connection.commit()
                return d
            else:
                connection.commit()
        finally:
            cursor.close()
            connection.close()

    def fast_import_df_to_sql(self, project_id, credentials, df, database_name, table_name, if_exists='append',
                              chunk_rows=50000):
        """
        Parameters
        ----------
        df : TYPE
            DESCRIPTION.
        database_name : STRING`
            Name of the Database to INSERT into i.e. Nero.
        table_name : STRING
            The name of the Database scheme and table name combined i.e. dh.test_table .
        if_exists : STRING, optional
            DESCRIPTION. The default is 'append', if wanted to overwrite the data in the table pass replace
        chunk_rows : INT, optional
            DESCRIPTION. The default is 50000.
        Returns
        -------
        None.
        """

        # Get the server name to link
        start_time = datetime.datetime.now()
        sql_client = BigQueryLoader(project_id, credentials)
        server_name = sql_client.exec_query("""select @@SERVERNAME""").to_string(index=False).lstrip()

        print('Data import to server:' + server_name + ' - ' + database_name + '.' + table_name + ' started')

        DTYPE_MAP = {
            "int64": "int",
            "int32": "int",
            "float64": "float",
            "object": "varchar(255)",
            "datetime64[ns]": "datetime",
            "bool": "bit"
        }

        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=' + server_name + ';'
                              'Database=' + database_name + ';'
                              'Trusted_Connection=yes;'

                              )

        def _get_data_types(df):
            """Get data types for each column as dictionary
            Handles default data type assignment and custom data types
            """
            data_types = {}
            for c in list(df.columns):
                dtype = str(df[c].dtype)
                if dtype not in DTYPE_MAP:
                    data_types[c] = "varchar(255)"
                else:
                    data_types[c] = DTYPE_MAP[dtype]
            return data_types

        def _get_schema(table_name):
            """Get schema and table name - returned as tuple
            """
            t_spl = table_name.split(".")
            if len(t_spl) > 1:
                return t_spl[0], ".".join(t_spl[1:])
            else:
                return '', table_name

        def _check_exists(cur, schema, table):
            """Check in conn if table exists
            """
            exist_or_not = cur.execute(
                f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' and TABLE_SCHEMA = '{schema}') select 1 else select 0"
            ).fetchall()[0][0]

            return exist_or_not

        def _generate_create_statement(schema, table, cols):
            """Generates a create statement
            """
            cols = ",".join([f'\n\t{k} {v}' for k, v in cols.items()])
            schema_and_table = f"[{schema}].[{table}]"
            create_statement = f"create table {schema_and_table} \n({cols}\n)"

            return create_statement

        def data_import_main(df, conn, table_name, method=if_exists):

            # Assign data types
            data_types = _get_data_types(df)

            # Get schema
            cur = conn.cursor()

            schema, name = _get_schema(table_name)

            if schema == '':
                schema = cur.execute("SELECT SCHEMA_NAME()").fetchall()[0][0]

            exists = _check_exists(cur, schema, name)

            # Handle existing table
            create_statement = ''

            if exists:
                if method == "replace":
                    cur.execute(f"drop table [{schema}].[{name}]")
                    create_statement = _generate_create_statement(schema, name, data_types)
                    cur.execute(create_statement)

                elif (method != "append") & (method != "replace"):
                    print('Wrong parameter for if_exist method')

            else:
                create_statement = _generate_create_statement(schema, name, data_types)
                cur.execute(create_statement)

            insert_sql = f"insert into [{schema}].[{name}] values ({','.join(['?' for v in data_types])})"
            insert_cols = df.values.tolist()
            cur.fast_executemany = True
            cur.executemany(insert_sql, insert_cols)
            cur.close()

            return create_statement

        # split the whole dataframe into small pieces to avoid too large data or memory errors happend (so far the 
        # testing result show 50000 at once is good) 
        df_length = len(df)
        split_df_total = math.floor(df_length / chunk_rows)

        # loop through each small dataframe and import it into the assigned table
        for i in range(0, split_df_total + 1):
            if i == split_df_total:
                split_df = df[split_df_total * chunk_rows:]
            else:
                split_df = df[i * chunk_rows:(i + 1) * chunk_rows]

            # official import start from here and set up the required settings to connect to SQL server
            if (len(split_df) > 0) & (if_exists == 'replace') & (i == 0):

                conn = pyodbc.connect('Driver={SQL Server};'
                                      'Server=' + server_name + ';'
                                      'Database=' + database_name + ';'
                                      'Trusted_Connection=yes;'
                                      )

                # the main function: createt the execute statement which you're going to do for SQL server
                create_statement = data_import_main(split_df, conn, table_name, method=if_exists)
                conn.commit()
                conn.close()

            elif len(split_df) > 0:
                conn = pyodbc.connect('Driver={SQL Server};'
                                      'Server=' + server_name + ';'
                                      'Database=' + database_name + ';'
                                      'Trusted_Connection=yes;'
                                      )

                # the main function: createt the execute statement which you're going to do for SQL server
                create_statement = data_import_main(split_df, conn, table_name, method="append")
                conn.commit()
                conn.close()

            else:
                None

        end_time = datetime.datetime.now()
        total_running_time = end_time - start_time
        print('Data import to server:' + server_name + ' - ' + database_name + '.' + table_name + ' completed')
        print('total running time: ' + str(total_running_time))