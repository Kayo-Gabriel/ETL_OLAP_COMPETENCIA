from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'kayo_unisales',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='1_carga_dimensoes_moderna',
    default_args=default_args,
    description='Carga das tabelas dimensões no padrão TaskFlow API do Airflow 3',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['rh', 'unisales', 'taskflow'],
)
def pipeline_dimensoes():

    @task
    def carregar_departamentos():
        sql_hook = MsSqlHook(mssql_conn_id='sqlserver_oltp')
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')
        df = sql_hook.get_pandas_df("SELECT DepartmentID, Name, GroupName FROM HumanResources.Department;")
        if not df.empty:
            df['Name'] = df['Name'].str.strip()
            df['GroupName'] = df['GroupName'].str.strip()
            registros = df.to_records(index=False).tolist()
            pg_hook.insert_rows(
                table='dw.ddepartment', rows=registros,
                target_fields=['DepartmentID', 'Name', 'GroupName'],
                replace=True, replace_index='DepartmentID'
            )
        return "Departamentos carregados!"

    @task
    def carregar_turnos():
        sql_hook = MsSqlHook(mssql_conn_id='sqlserver_oltp')
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')
        df = sql_hook.get_pandas_df("SELECT ShiftID, Name, StartTime, EndTime FROM HumanResources.Shift;")
        if not df.empty:
            df['Name'] = df['Name'].str.strip()
            df['StartTime'] = df['StartTime'].astype(str)
            df['EndTime'] = df['EndTime'].astype(str)
            registros = df.to_records(index=False).tolist()
            pg_hook.insert_rows(
                table='dw.dshift', rows=registros,
                target_fields=['ShiftID', 'Name', 'StartTime', 'EndTime'],
                replace=True, replace_index='ShiftID'
            )
        return "Turnos carregados!"

    @task
    def carregar_pagamentos():
        sql_hook = MsSqlHook(mssql_conn_id='sqlserver_oltp')
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')
        df = sql_hook.get_pandas_df("SELECT DISTINCT PayFrequency FROM HumanResources.EmployeePayHistory;")
        if not df.empty:
            df['PayHistoryID'] = range(1, len(df) + 1)
            registros = df[['PayHistoryID', 'PayFrequency']].to_records(index=False).tolist()
            pg_hook.insert_rows(
                table='dw.dpay_history', rows=registros,
                target_fields=['PayHistoryID', 'PayFrequence'],
                replace=True, replace_index='PayHistoryID'
            )
        return "Histórico de pagamentos carregado!"

    @task
    def carregar_funcionarios():
        sql_hook = MsSqlHook(mssql_conn_id='sqlserver_oltp')
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')

        query = """
            SELECT
                BusinessEntityID as EmployeeID,
                NationalIDNumber,
                LoginID,
                CAST(OrganizationNode AS VARCHAR(100)) as OrganizationNode,
                OrganizationLevel,
                JobTitle,
                MaritalStatus,
                Gender,
                BirthDate,
                HireDate,
                SalariedFlag,
                CurrentFlag
            FROM HumanResources.Employee;
        """
        df = sql_hook.get_pandas_df(query)
        if not df.empty:
            df['LoginID'] = df['LoginID'].str.strip()
            df['JobTitle'] = df['JobTitle'].str.strip()
            df['BirthDate'] = df['BirthDate'].astype(str)
            df['HireDate'] = df['HireDate'].astype(str)

            df['SalariedFlag'] = df['SalariedFlag'].astype(int)
            df['CurrentFlag'] = df['CurrentFlag'].astype(int)

            df = df.astype(object).where(pd.notnull(df), None)
            colunas_ordenadas = [
                'EmployeeID', 'NationalIDNumber', 'LoginID', 'OrganizationNode',
                'OrganizationLevel', 'JobTitle', 'MaritalStatus', 'Gender',
                'BirthDate', 'HireDate', 'SalariedFlag', 'CurrentFlag'
            ]
            df = df[colunas_ordenadas]
            registros = df.to_records(index=False).tolist()

            pg_hook.run("TRUNCATE TABLE dw.demployee RESTART IDENTITY CASCADE;")
            pg_hook.insert_rows(
                table='dw.demployee',
                rows=registros,
                target_fields=colunas_ordenadas
            )
        return "Funcionários carregados!"

    @task
    def carregar_calendario():
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')
        tem_dados = pg_hook.get_first("SELECT count(1) FROM dw.ddate;")[0]

        if tem_dados == 0:
            dates = pd.date_range(start='2000-01-01', end='2030-12-31', freq='D')
            df = pd.DataFrame({
                'FullDate': dates.date.astype(str),
                'Day': dates.day,
                'Month': dates.month,
                'Year': dates.year,
                'Semester': (dates.month - 1) // 6 + 1
            })
            registros = df.to_records(index=False).tolist()
            # Omitimos o DateID no target_fields pois ele é serial (gerado automaticamente pelo banco)
            pg_hook.insert_rows(
                table='dw.ddate',
                rows=registros,
                target_fields=['FullDate', 'Day', 'Month', 'Year', 'Semester']
            )
        return "Calendário carregado (ou já existia)!"

    @task
    def carregar_relogio():
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')
        tem_dados = pg_hook.get_first("SELECT count(1) FROM dw.dtime;")[0]

        if tem_dados == 0:
            times = pd.date_range("00:00:00", "23:59:59", freq="s")
            df = pd.DataFrame({
                'FullTime': times.time.astype(str),
                'Hour': times.hour,
                'Minute': times.minute,
                'Second': times.second
            })
            registros = df.to_records(index=False).tolist()
            # Omitimos o TimeID no target_fields pois ele é serial (gerado automaticamente pelo banco)
            pg_hook.insert_rows(
                table='dw.dtime',
                rows=registros,
                target_fields=['FullTime', 'Hour', 'Minute', 'Second']
            )
        return "Relógio carregado (ou já existia)!"

    @task
    def carregar_candidato_padrao():
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')
        tem_dados = pg_hook.get_first("SELECT count(1) FROM dw.djob_candidate WHERE JobCandidateID = 1;")

        if not tem_dados or tem_dados[0] == 0:
            df = pd.DataFrame({
                'JobCandidateID': [1],
                'Resume': ['Não Aplicável / Sem Currículo']
            })
            registros = df.to_records(index=False).tolist()
            pg_hook.insert_rows(
                table='dw.djob_candidate',
                rows=registros,
                target_fields=['JobCandidateID', 'Resume'],
                replace=True,
                replace_index='JobCandidateID'
            )
        return "Candidato padrão carregado!"

    # Orquestração das tarefas
    dep = carregar_departamentos()
    shift = carregar_turnos()
    pay = carregar_pagamentos()
    emp = carregar_funcionarios()
    cal = carregar_calendario()
    rel = carregar_relogio()
    cand = carregar_candidato_padrao()

    [dep, shift, pay, emp, cal, rel, cand]

pipeline_dimensoes()
