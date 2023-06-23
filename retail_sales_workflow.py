import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.blocks.system import Secret
from sqlalchemy import create_engine

stg_con = Secret.load('stg-connection').get()
stg_engine = create_engine(stg_con)

dev_con = Secret.load('dev-connection').get()
dev_engine = create_engine(dev_con)


@task()
def sales_figures_read_excel(file: str, sheet: str) -> pd.DataFrame:
    """Load sales figures dataframe and drop empty column"""
    df_sales = pd.read_excel(file, sheet_name=sheet, header=1)
    df_sales.drop('Customer', axis=1, inplace=True)
    return df_sales


@task()
def sales_figures_fix_datatype(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Fix some data type issues"""
    df_sales = df_sales[df_sales['MonthYear'] != ' - - - - ']
    df_sales['MonthYear'] = pd.to_datetime('01.' + df_sales['MonthYear'], dayfirst=True)
    df_sales[['Time index', 'StoreID', 'Dept_ID']] = df_sales[['Time index', 'StoreID', 'Dept_ID']].astype(int)
    df_sales.loc[df_sales['Area (m2)'] == '#NV', 'Area (m2)'] = np.nan
    df_sales['Area (m2)'] = df_sales['Area (m2)'].astype(float)
    df_sales.loc[df_sales['HoursOwn'] == '?', 'HoursOwn'] = np.nan
    df_sales['HoursOwn'] = df_sales['HoursOwn'].astype(float)
    return df_sales


@task()
def sales_figures_fill_missing_values(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Fill missing values in sales figures dataframe with mean value by store and department"""
    df_mean_area = df_sales.groupby(['StoreID', 'Dept_ID'])['Area (m2)'].transform(np.mean)
    df_sales['Area (m2)'].fillna(df_mean_area, inplace=True)
    df_mean_hours_own = df_sales.groupby(['StoreID', 'Dept_ID'])['HoursOwn'].transform(np.mean)
    df_sales['HoursOwn'].fillna(df_mean_hours_own, inplace=True)
    return df_sales


@task()
def sales_figures_rename_columns(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Rename sales figures dataframe's columns to make working with PostgreSQL easier"""
    df_sales.columns = ['month_year', 'time_index', 'country', 'store_id', 'city', 'dept_id', 'dept_name',
                        'hours_own', 'hours_lease', 'sales_units', 'turnover', 'area', 'opening_hours']
    return df_sales


@task()
def write_to_postgres(df: pd.DataFrame, engine: create_engine, table_name: str) -> None:
    """Upload dataframe to database"""
    df.to_sql(table_name,
              con=engine,
              schema='public',
              if_exists='replace',
              index=False)
    return


@task()
def opening_schemes_read_excel(file: str, sheet: str) -> pd.DataFrame:
    """Load opening schemes dataframe"""
    df_schemes = pd.read_excel(file, sheet_name=sheet)
    return df_schemes


@task()
def opening_schemes_normalize(df_schemes: pd.DataFrame) -> pd.DataFrame:
    """Delete empty rows and transform the dataframe into a normalized table"""
    df_schemes.iloc[4, 4:] = df_schemes.iloc[3, 4:]
    df_schemes.columns = df_schemes.iloc[4, :]
    df_schemes = df_schemes.iloc[5:, :]
    df_cumulated = pd.concat([df_schemes.iloc[:, :4], df_schemes.iloc[:, 17:]], axis=1)
    df_cumulated['value_type'] = 'cumulated'
    df_schemes = pd.concat([df_schemes.iloc[:, :16], df_cumulated], axis=0)
    df_schemes['value_type'].fillna('month', inplace=True)
    df_schemes = df_schemes.melt(['id', 'Store name', 'Region', 'Scheme', 'value_type'],
                                 var_name='period',
                                 value_name='value')
    return df_schemes


@task()
def opening_schemes_fix_datatype(df_schemes: pd.DataFrame) -> pd.DataFrame:
    """Fix some data type issues"""
    df_schemes['period'] = pd.to_datetime('1.' + df_schemes['period'].astype(str), dayfirst=True)
    df_schemes['id'] = df_schemes['id'].astype(int)
    df_schemes['value'] = df_schemes['value'].astype(float)
    return df_schemes


@task()
def opening_schemes_rename_columns(df_schemes: pd.DataFrame) -> pd.DataFrame:
    """Rename sales figures dataframe's columns to make working with PostgreSQL easier"""
    df_schemes.columns = ['store_id', 'store_name', 'region', 'opening_hours', 'value_type', 'month_year', 'value']
    return df_schemes


@task()
def merge_dataframes() -> pd.DataFrame:
    """Merge two dataframes on store id and period"""
    df_sales = pd.read_sql_table(table_name='sales_figures', con=stg_con, schema='public')
    df_schemes = pd.read_sql_table(table_name='opening_schemes', con=stg_con, schema='public')
    df = df_sales.merge(df_schemes, how='left', on=['store_id', 'month_year', 'opening_hours'])
    return df


@flow(name='Sales Figures Subflow')
def sales_figures_subflow():
    """Combine tasks to load osales figures dataset"""
    df_sales = sales_figures_read_excel(file='salesworkload.xlsx', sheet='sales_figures')
    df_sales = sales_figures_fix_datatype(df_sales)
    df_sales = sales_figures_fill_missing_values(df_sales)
    df_sales = sales_figures_rename_columns(df_sales)
    write_to_postgres(df=df_sales, engine=stg_engine, table_name='sales_figures')
    return


@flow(name='Opening Schemes Subflow')
def opening_schemes_subflow():
    """Combine tasks to load opening schemes dataset"""
    df_schemes = opening_schemes_read_excel(file='salesworkload.xlsx', sheet='opening_schemes')
    df_schemes = opening_schemes_normalize(df_schemes)
    df_schemes = opening_schemes_fix_datatype(df_schemes)
    df_schemes = opening_schemes_rename_columns(df_schemes)
    write_to_postgres(df=df_schemes, engine=stg_engine, table_name='opening_schemes')
    return


@flow(name='Merged Dataframes Subflow')
def merged_dataframes_subflow():
    """Merge the data from public.sales_figures and public.opening_schemes into one dataset
       and load into dev database"""
    df = merge_dataframes()
    write_to_postgres(df=df, engine=dev_engine, table_name='retail_sales_data')
    return


@flow(name='Main Flow')
def main_flow():
    """Main ETL function"""
    sales_figures_subflow()
    opening_schemes_subflow()
    merged_dataframes_subflow()


if __name__ == '__main__':
    main_flow()
