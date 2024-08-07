import pandas as pd
import sqlglot
from datetime import datetime, timedelta
import time
import os

from sqlglot import parse_one, exp, optimizer
from sqlglot.optimizer import qualify_columns, qualify_tables, isolate_table_selects
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.errors import ParseError, OptimizeError
import pandas as pd


def qualify_columns(expression, schema):
    try:
        # print(repr(expression))
        expression = optimizer.qualify_tables.qualify_tables(expression)
        # print(repr(expression))
        expression = optimizer.isolate_table_selects.isolate_table_selects(expression)
        # print(repr(expression))
        expression = optimizer.qualify_columns.qualify_columns(expression, schema)
        # print(repr(expression))
    #
    except (OptimizeError) as e:
        pass

    return expression


sql_query = """
WITH SalesSummary AS (
    SELECT p.Product_Name, SUM(s.Quantity) AS Total_Sales
    FROM Products p
    JOIN salve.Sales s ON p.Product_ID = s.Product_ID
    WHERE s.Sale_Date BETWEEN TO_DATE('2021-01-01', 'YYYY-MM-DD') AND TO_DATE('2021-12-31', 'YYYY-MM-DD')
    GROUP BY p.Product_Name
), Suport as (SELECT * from schema.sup)
SELECT ss.Product_Name, ss.Total_Sales, c.Category_Name
FROM SalesSummary ss
JOIN Categories c ON ss.Product_Name = c.Product_Name
where ss.Product_Name not in (select * from sca.product_item as PI)
ORDER BY ss.Total_Sales DESC;
"""
sql_query = "SELECT * FROM sa.tableA inner join tableB b on a.cod = b.code inner join tableC as c on b.id = c.id WHERE b.Product_ID = 1"


# sql_query = "select TBA.column as CCCOO from schemaA.tableA as TBA"

def lazzy(n, l):
    for i in l:
        if not i.startswith("__"):
            print(f"print({n}.{i})")


def parse_analysis(sql_query, dialect):
    ast = sqlglot.parse_one(sql_query, read=dialect)
    ast = qualify_columns(ast, schema=None)

    kind_analysis = []

    for scope in traverse_scope(ast):
        kind_analysis.append((scope.is_cte, scope.is_root, scope.is_subquery, scope.is_correlated_subquery,
                              scope.is_derived_table, scope.is_union, scope.is_udtf))

    return kind_analysis


def parse_statement(sql_query, dialect):
    ast = sqlglot.parse_one(sql_query, read=dialect)
    ast = qualify_columns(ast, schema=None)

    # print(repr(ast.args["from"].this))
    # print(ast.args["from"].this.db, ast.args["from"].this.this, ast.args["from"].this.alias)

    # physical_columns = [(scope.sources.get(c.table).name, c.name)for scope in traverse_scope(ast)for c in scope.columnsif isinstance(scope.sources.get(c.table), exp.Table)]
    physical_columns = []
    stmt_index = 0

    for scope in traverse_scope(ast):
        # aqui da para contar os batches
        # check CTEs
        # print(scope.is_cte)
        # for i in ((scope.ctes)):
        #     print((i.alias))

        # if not scope.columns:

        # physical_columns.append((scope.sources.get(c.table).db, scope.sources.get(c.table).name, c.table, c.name))
        for c in scope.columns:
            if c.table:
                if isinstance(scope.sources.get(c.table), exp.Table):
                    schema = "default" if not scope.sources.get(c.table).db else scope.sources.get(c.table).db
                    table = scope.sources.get(c.table).name
                    fully = f"{table}" if schema == "default" else f"{schema}.{table}"
                    physical_columns.append((stmt_index, schema, table, fully, c.table, c.name))
        stmt_index += 1
    return physical_columns


def get_lineage_df(sql_query: str, dialect):
    lineage = parse_statement(sql_query, dialect)
    df = pd.DataFrame(lineage, columns=['StatementIndex', 'SchemaName', 'PhysicalTableName', 'FullyIdentifierTableName',
                                        'TableIdentifier', 'ColumnName'])
    return df


def get_analysis_df(sql_query, dialect):
    analysis = parse_analysis(sql_query, dialect)
    df = pd.DataFrame(analysis, columns=['is_cte', 'is_root', 'is_subquery', 'is_correlated_subquery',
                              'is_derived_table', 'is_union', 'is_udtf'])

    return df

a = parse_statement(sql_query, 'oracle')
print(a)
# print(type(a))
