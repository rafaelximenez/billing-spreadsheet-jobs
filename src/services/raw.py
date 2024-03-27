from google.cloud import bigquery

import google.auth
import pandas_gbq as pdgbq
import pandas as pd
import re

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)

def gen_bq_table(tablename):
    print(f'Lendo a tabela - {tablename}')
    pdgbq.context.credentials = credentials
    bigquery.Client(project, credentials)
    query = f"SELECT * FROM `voltaic-country-351414.bpool_base.{tablename}`"
    return pdgbq.read_gbq(query, project_id="voltaic-country-351414", credentials=credentials)

def format_to_currency(value):
    value = re.sub('[a-zA-Z]', '', str(value)).replace('R$', '').replace('$', '').replace('.', '').replace(',', '.')
    value = value.replace('#VALUE!', '').replace('#REF!', '').replace('-', '').replace('#!', '')
    try: return float(value)
    except: return 0

def calculate_brl_value(row, df_dolar):    
    filtered_df = df_dolar[df_dolar['competence'] == row['month']]
    dolar = 1
    if len(filtered_df) > 0:
        dolar = float(str(filtered_df['value'].values[0]).replace(',', '.'))
    
    row['dolar'] = dolar
    row['agency_invoice_value_brl']      = round(float(row['b_pool_invoice_withoutiva_usd']) * dolar, 2)
    row['b_pool_invoice_withoutiva_brl'] = round(row['bpool_invoice_sin_iva_us'] * dolar, 2)
    row['gmv'] = round(row['agency_invoice_value_brl'] + row['b_pool_invoice_withoutiva_brl'], 2)
    row['revenue'] = round(row['b_pool_invoice_withoutiva_brl'], 2) 
    return row

def calculate_brl_value_usa(row, df_dolar):
    filtered_df = df_dolar[df_dolar['competence'] == row['month']]
    dolar = 1
    if len(filtered_df) > 0:
        dolar = float(str(filtered_df['value'].values[0]).replace(',', '.'))
    row['dolar'] = dolar
    row['gmv'] = round(row['po_value'] * dolar, 2)
    row['revenue'] = round(row['feebpool'] * dolar, 2)
    return row
def format_region(value):
    region = 'Sem região'
    if value == 'COl': region = 'Colombia'
    if value == 'CR': region = 'Costa Rica'
    if value == 'EC': region = 'Equador'
    if value == 'MX': region = 'México'
    if value == 'AR': region = 'Argentina'
    if value == 'CH': region = 'Chile'
    if value == 'COL': region = 'Colômbia'
    if value == 'BZ': region = 'Brasil'
    if value == 'PR': region = 'Paraguai'
    if value == 'Pn': region = 'Panama'
    if value == 'UY': region = 'Uruguai'
    return region
def transform_latam(df, df_dolar):
    df['tablename'] = 'Nuevo Latam'
    
    df['client_id'] = ''
    df['sku']     = df['cd_project'].str.strip()
    df['client']  = df['client'].str.strip()
    df['project'] = df['project'].str.strip()
    df['b_pool_invoice_withoutiva_usd'] = df['agency_invoice_value_usd'].apply(format_to_currency).round(2)
    df['bpool_invoice_sin_iva_us'] = df['bpool_invoice_sin_iva_us'].apply(format_to_currency).round(2)
    df['billing_date'] = pd.to_datetime(df['billing_date'], dayfirst=True)
    df['date_nf'] = df['billing_date'].dt.strftime('%Y-%m-%d')
    df['month']   = df['billing_date'].dt.strftime('%Y-%m')
    df = df.apply(lambda row: calculate_brl_value(row, df_dolar), axis=1)
    df['adiant_revenue'] = 0
    df['partner'] = df['agency'].str.strip()
    df['gmv'] = df['agency_invoice_value_brl'] + df['b_pool_invoice_withoutiva_brl']
    df['gmv_partner'] = df['coluna_n_agencia'].apply(format_to_currency).round(2)
    df['region'] = df['country_code'].apply(format_region)
    df['module']  = df['module'].str.strip()
    return df

def transform_usa(df, df_dolar):
    df['tablename'] = 'USA'
    
    df['client_id'] = ''
    df['sku']     = df['sku'].str.strip()
    df['client']  = df['client'].str.strip()
    df['project'] = df['project'].str.strip()
    df['partner'] = df['partner'].str.strip()
    df['billing_date'] = pd.to_datetime(df['bpool_billing_date'], dayfirst=True)
    df['date_nf'] = df['billing_date'].dt.strftime('%Y-%m-%d')
    df['month']   = df['billing_date'].dt.strftime('%Y-%m')
    
    df['po_value'] = df['po_value'].apply(format_to_currency).round(2)    
    df['feebpool'] = df['feebpool'].apply(format_to_currency).round(2)      
    df = df.apply(lambda row: calculate_brl_value_usa(row, df_dolar), axis=1)
    df['adiant_revenue'] = 0
    df['gmv_partner'] = df['gmv']
    df['region'] = 'USA'
    df['module']  = ''
    return df

def apply_double_taxed(row, df):
    filtered_df = df[df['nf_code'] == row['sending_of_nf_no_nf']]
    if len(filtered_df) > 0:
        row['revenue'] = row['revenue'] - filtered_df['revenue_discount'].values[0] - filtered_df['fee_bpool'].values[0]
        print(row)
    return row

def transform_brasil(df, df2):
    df2['po_value'] = df2['po_value'].apply(format_to_currency).round(2)
    df2['fee_bpool'] = df2['fee_bpool'].apply(format_to_currency).round(2)
    df2['revenue_discount'] = df2['po_value'] - df2['fee_bpool']

    df = df[~df['month'].isin(['perda', 'substituição', 'desconsiderar', 'perda em fev/24'])]

    df['tablename'] = 'Brasil'
    df['sku']     = df['sku'].str.strip()
    df['client']  = df['client'].str.strip()
    df['project'] = df['project'].str.strip()
    df['gmv']      = df['po_value'].apply(format_to_currency).round(2)
    df['revenue']  = df['fee_bpool'].apply(format_to_currency).round(2)
    df['adiant_revenue'] = 0
    df['billing_date'] = pd.to_datetime(df['billing_data_b_pool'], dayfirst=True, errors='coerce')
    
    df['date_nf'] = df['month'] + '-01'
    df['month']        = df['month'].str.strip()    
    
    df['partner'] = df['partner'].str.strip()
    df['gmv_partner'] = df['nf_parceiro_value'].apply(format_to_currency).round(2)
    df['region'] = 'Brasil'
    df['module']  = df['module'].str.strip()
    
    df = df.apply(lambda row: group_via_nfs(row), axis=1)
    df = df.apply(lambda row: apply_double_taxed(row, df2), axis=1)

    return df

def group_via_nfs(row):
    if row['client'] is None: return row
    
    if row['client'].upper() == "VIA":
        row['project'] = f"Projeto - {row['po']}"
        
    return row

def transform_adiant(df):
    df = df.dropna(subset=['month'])

    df['tablename'] = 'Brasil' 
    df['sku'] = '' 
    df['client'] = '' 
    df['client_id'] = '' 
    df['project'] = '' 
    df['gmv'] = 0 
    df['revenue'] = 0 
    df['gmv_partner'] = 0 
    df['adiant_revenue'] = df['value_brl'].apply(format_to_currency).round(2)
    df['date_nf']      = df['month'] + '-01'
    df['month']        = df['month'].str.strip()
    df['billing_date'] = '' 
    df['partner'] = '' 
    df['region'] = '' 
    df['module'] = ''
    return df

def transform_ajustes(df):
    df['tablename'] = 'Brasil' 
    df['sku'] = '' 
    df['client'] = '' 
    df['client_id'] = '' 
    df['project'] = '' 
    df['gmv'] = -df['po_value'].apply(format_to_currency).round(2)
    df['revenue'] = -df['fee_bpool'].apply(format_to_currency).round(2)
    df['gmv_partner'] = 0 
    df['adiant_revenue'] = 0
    df['date_nf']      = df['month'] + '-01'
    df['month']        = df['month'].str.strip()
    df['billing_date'] = '' 
    df['partner'] = '' 
    df['region'] = '' 
    df['module'] = ''
    return df
def delete_data_from_bigquery_table(project_id, dataset_id, table_id, condition=None):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        delete_query = f"DELETE FROM `{table_ref}`"
        if condition:
            delete_query += f" WHERE {condition}"

        query_job = client.query(delete_query)
        query_job.result()
        return True
    except Exception as e:
        return False
def get_tb_schema( project_id, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
    table = client.get_table(table_ref)
    schema = table.schema
    return [{"name": field.name, "type": field.field_type} for field in schema]
    
def convert_cols_types(df, schema_for_pandas):
    type_mapping = {
        'STRING': 'str',
        'INTEGER': 'int64',
        'FLOAT': 'float64',
        'BOOLEAN': 'bool',
        'DATE': 'datetime64',
        'TIMESTAMP': 'datetime64'
    }
    for field in schema_for_pandas:
        column_name = field['name']
        column_type = field['type']
        if column_name in df.columns:
            if column_type == 'DATE':
                df[column_name] = pd.to_datetime(df[column_name]).dt.date
            else:
                df[column_name] = df[column_name].astype(type_mapping[column_type])
    return df