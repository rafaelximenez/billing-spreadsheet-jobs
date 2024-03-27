from dotenv import load_dotenv
load_dotenv()

from services.raw import *

import google.auth
import pandas_gbq as pdgbq
import pandas as pd

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)

def  start():
    project_id = "voltaic-country-351414"
    dataset_id = "bpool_base"
    allow_cols = ['tablename', 'sku', 'client_id', 'client', 'project', 'gmv', 'revenue', 'adiant_revenue', 'date_nf', 'month', 'billing_date', 'partner', 'gmv_partner', 'region', 'module']
    df_dolar  = gen_bq_table('bp_dollar_quotes')
    
    df_latam  = gen_bq_table('bp_gsheet_nfs_nuevo_latam')
    df_latam  = transform_latam(df_latam, df_dolar)
    test_cols = ['tablename', 'sku', 'client_id', 'client', 'project', 'gmv', 'revenue', 'dolar', 'agency_invoice_value_usd', 'agency_invoice_value_brl', 'b_pool_invoice_withoutiva_usd' , 'b_pool_invoice_withoutiva_brl', 'adiant_revenue', 'date_nf', 'month', 'partner', 'gmv_partner', 'region', 'module']
    
    
    table_id = "bp_gsheet_nfs_latam_test"

    print(f'Salvando a tabela - {table_id}')
    pdgbq.to_gbq(df_latam[test_cols], f'{dataset_id}.{table_id}', project_id, if_exists='replace', credentials=credentials )
    df_latam = df_latam[allow_cols]
       
    df_usa  = gen_bq_table('bp_gsheet_nfs_usa')
    df_usa  = transform_usa(df_usa, df_dolar)
    
    test_cols = ['tablename', 'sku', 'client_id', 'client', 'project', 'gmv', 'revenue', 'dolar', 'po_value', 'adiant_revenue', 'date_nf', 'month', 'partner', 'gmv_partner', 'region', 'module']
    
    
    table_id = "bp_gsheet_nfs_usa_test"
    print(f'Salvando a tabela - {table_id}')
    pdgbq.to_gbq(df_usa[test_cols], f'{dataset_id}.{table_id}', project_id, if_exists='replace', credentials=credentials )
    df_usa = df_usa[allow_cols]
    
    df_double_taxed = gen_bq_table('bp_gsheet_nfs_double_taxed')
    df_brasil = gen_bq_table('bp_gsheet_nfs_brasil')
    df_brasil = transform_brasil(df_brasil, df_double_taxed)
    df_brasil = df_brasil[allow_cols]
    table_id = "bp_gsheet_nfs_brasil_test"
    print(f'Salvando a tabela - {table_id}')
    pdgbq.to_gbq(df_brasil, f'{dataset_id}.{table_id}', project_id, if_exists='replace', credentials=credentials )
    
    df_adiant = gen_bq_table('bp_gsheet_nfs_adiantiment_revenue')
    df_adiant = transform_adiant(df_adiant)
    df_adiant = df_adiant[allow_cols]

    df_ajustes = gen_bq_table('bp_gsheet_nfs_ajustes')
    df_ajustes = transform_ajustes(df_ajustes)
    df_ajustes = df_ajustes[allow_cols]
    df = pd.concat([df_latam, df_usa, df_brasil, df_adiant, df_ajustes])
    df = df.dropna(subset=['month'])
    table_id = "bp_gsheet_nfs"
    schema_for_pandas = get_tb_schema(project_id, dataset_id, table_id)
    df = convert_cols_types(df, schema_for_pandas)
    
    print(f'Salvando a tabela - {table_id}')
    pdgbq.to_gbq(df, f'{dataset_id}.{table_id}', project_id, if_exists='replace', credentials=credentials )
    
start()