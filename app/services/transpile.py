import sqlglot


def beauty_transpiler(query: str, read: str, write: str):
    stmt = sqlglot.transpile(query, read=read, write=write, pretty=True)
    return stmt[0]