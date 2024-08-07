from sqlglot import exp
from sqlglot import parse_one

sql_query = "SELECT * FROM sa.tableA inner join tableB b on a.cod = b.code and a.ref_date = b.date inner join tableC as c on b.id = c.id WHERE b.Product_ID = 1"


ast = parse_one(sql_query)

# This is NOT a good way to find all tables in the query!
for table in ast.find_all(exp.Join):
    print((table.this))
