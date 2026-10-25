from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'kayo_unisales',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='2_carga_fato_incremental',
    default_args=default_args,
    description='Carga incremental da tabela fato no padrão do professor usando Airflow Variables',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['rh', 'unisales', 'fato', 'incremental'],
)
def pipeline_fato():

    @task
    def etl_tabela_fato():
        sql_hook = MsSqlHook(mssql_conn_id='sqlserver_oltp')
        pg_hook = PostgresHook(postgres_conn_id='postgres_olap')

        ultima_data_carga = Variable.get("ultima_data_carga_fato", default_var="1900-01-01")
        print(f"Buscando registros modificados após: {ultima_data_carga}")

        query_origem = f"""
            SELECT
                eph.BusinessEntityID AS EmployeeID,
                eph.RateChangeDate,
                eph.Rate,
                eph.PayFrequency AS PayHistoryID,
                eph.ModifiedDate,
                e.VacationHours,
                e.SickLeaveHours
            FROM HumanResources.EmployeePayHistory eph
            INNER JOIN HumanResources.Employee e ON eph.BusinessEntityID = e.BusinessEntityID
            WHERE eph.ModifiedDate > '{ultima_data_carga}';
        """
        df_fato = sql_hook.get_pandas_df(query_origem)

        if df_fato.empty:
            print("Nenhum dado novo encontrado no SQL Server para incremento.")
            return "Fato atualizada (Sem dados novos)"

        df_fato['FullDate'] = pd.to_datetime(df_fato['RateChangeDate']).dt.date.astype(str)
        df_fato['FullTime'] = pd.to_datetime(df_fato['RateChangeDate']).dt.time.astype(str)

        #df_fato['RateChangeDate'] = df_fato['RateChangeDate'].astype(str)

        #df_ddate = pg_hook.get_pandas_df("SELECT DateID, FullDate FROM dw.ddate;")
        #df_dtime = pg_hook.get_pandas_df("SELECT TimeID, FullTime FROM dw.dtime;")

        df_ddate = pg_hook.get_pandas_df("SELECT DateID, FullDate FROM dw.ddate;")
        df_ddate.columns = ['DateID', 'FullDate'] # Garante o nome correto no Pandas

        df_dtime = pg_hook.get_pandas_df("SELECT TimeID, FullTime FROM dw.dtime;")
        df_dtime.columns = ['TimeID', 'FullTime'] # Garante o nome correto no Pandas

        df_ddate['FullDate'] = df_ddate['FullDate'].astype(str)
        df_dtime['FullTime'] = df_dtime['FullTime'].astype(str)

        df_fato = pd.merge(df_fato, df_ddate, on='FullDate', how='left')
        df_fato = pd.merge(df_fato, df_dtime, on='FullTime', how='left')

        df_aux = sql_hook.get_pandas_df("""
            SELECT BusinessEntityID AS EmployeeID, DepartmentID, ShiftID
            FROM HumanResources.EmployeeDepartmentHistory
            WHERE EndDate IS NULL;
        """)

        df_final = pd.merge(df_fato, df_aux, on='EmployeeID', how='left')
        df_final['JobCandidateID'] = 1
        df_final = df_final.astype(object).where(pd.notnull(df_final), None)

        colunas_finais = [
            'DateID', 'TimeID', 'DepartmentID', 'EmployeeID', 'PayHistoryID',
            'ShiftID', 'JobCandidateID', 'Rate', 'VacationHours', 'SickLeaveHours'
        ]

        df_final = df_final[colunas_finais]
        registros_fato = df_final.to_records(index=False).tolist()

        #colunas_fato = [
        #    'EmployeeID', 'DepartmentID', 'ShiftID', 'PayHistoryID',
        #    'JobCandidateID', 'RateChangeDate', 'Rate'
        #]
        #df_final = df_final[colunas_fato]
        #registros_fato = df_final.to_records(index=False).tolist()

        #colunas_postgres = [
        #    'EmployeeID', 'DepartmentID', 'ShiftID', 'PayHistoryID',
        #    'JobCandidateID', 'date', 'rate'
        #]

        pg_hook.insert_rows(
            table='dw.fhuman_resources',
            rows=registros_fato,
            target_fields=colunas_finais
        )
        print(f"Sucesso! {len(registros_fato)} novos registros inseridos na tabela fato.")

        nova_data_limite = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Variable.set("ultima_data_carga_fato", nova_data_limite)
        print(f"Variável de controle incremental 'ultima_data_carga_fato' atualizada para: {nova_data_limite}")

        return "Carga Fato Incremental Concluída!"

    etl_tabela_fato()

pipeline_fato()
