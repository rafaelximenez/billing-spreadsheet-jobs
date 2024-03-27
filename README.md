# Pipeline de Transformação de Dados

Este script realiza transformações em dados provenientes do Google BigQuery e os armazena em diferentes formatos. Ele é composto por diversas funções que executam tarefas específicas de transformação e manipulação dos dados.

## Funcionalidades

- **Conexão ao BigQuery**: O script estabelece uma conexão com o Google BigQuery para recuperar dados de diferentes tabelas.
- **Transformações de Dados**: Executa diversas transformações nos dados, como formatação de valores monetários, cálculo de valores em diferentes moedas, e formatação de datas.
- **Normalização de Regiões**: Normaliza os códigos de região para nomes de países.
- **Leitura e Escrita em BigQuery**: Oferece funcionalidades para deletar dados de tabelas existentes no BigQuery e obter o esquema de uma tabela.
- **Conversão de Tipos de Dados**: Converte os tipos de dados das colunas de acordo com o esquema fornecido.

## Descrição das Funções

1. **`gen_bq_table(tablename)`**: Lê os dados de uma tabela específica do BigQuery e retorna um DataFrame do Pandas.
2. **`format_to_currency(value)`**: Formata valores monetários para o padrão utilizado no script.
3. **`calculate_brl_value(row, df_dolar)`**: Calcula valores em Real brasileiro (BRL) com base nos valores em dólar e na taxa de câmbio.
4. **`calculate_brl_value_usa(row, df_dolar)`**: Calcula valores em dólar com base nos valores em outras moedas e na taxa de câmbio do dólar.
5. **`format_region(value)`**: Normaliza códigos de região para nomes de países.
6. **`transform_latam(df, df_dolar)`**: Realiza transformações específicas para dados da América Latina.
7. **`transform_usa(df, df_dolar)`**: Realiza transformações específicas para dados dos Estados Unidos.
8. **`apply_double_taxed(row, df)`**: Aplica correções em valores de receita que foram duplamente tributados.
9. **`transform_brasil(df, df2)`**: Realiza transformações específicas para dados do Brasil.
10. **`group_via_nfs(row)`**: Agrupa dados de projetos específicos.
11. **`transform_adiant(df)`**: Realiza transformações específicas para dados de adiantamentos.
12. **`transform_ajustes(df)`**: Realiza transformações específicas para dados de ajustes.
13. **`delete_data_from_bigquery_table(project_id, dataset_id, table_id, condition=None)`**: Deleta dados de uma tabela do BigQuery.
14. **`get_tb_schema(project_id, dataset_id, table_id)`**: Obtém o esquema de uma tabela do BigQuery.
15. **`convert_cols_types(df, schema_for_pandas)`**: Converte os tipos de dados das colunas de acordo com o esquema fornecido.

## Uso

1. Configure as credenciais do Google Cloud Platform para autenticação.
2. Execute as funções conforme necessário para transformar e manipular os dados.

## Observações

- Este script assume que os dados estão armazenados no BigQuery e que as tabelas estão acessíveis.
- É necessário configurar as variáveis de ambiente para autenticação no Google Cloud Platform.

## Benefícios

- Automatiza processos de transformação e manipulação de dados.
- Permite realizar transformações específicas para diferentes regiões e tipos de dados.
- Facilita a integração e a interação com dados armazenados no Google BigQuery.

## Melhorias Futuras

- Implementar tratamento de erros e logging para melhorar a robustez do script.
- Adicionar suporte para transformações adicionais e novos tipos de dados.
- Refatorar o código para melhorar a legibilidade e modularidade.
