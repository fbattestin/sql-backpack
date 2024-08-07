import time
import json
import streamlit as st
from services.transpile import beauty_transpiler
from services.dialects import available_dialects
from services.void import get_lineage_df, get_analysis_df
import plotly.express as px

DIALECTS: tuple = available_dialects()


def streamer(txt: str):
    for word in txt:
        yield word
        time.sleep(0.005)


@st.dialog("Paser Error", width="large")
def parser_error(error):
    # st.write(dir(error.errors))
    st.write("```\n" + str(error) + "\n```")

@st.dialog("SQL Analysis", width="large")
def sql_analysis_msg():
    # st.write(dir(error.errors))
    st.write("Essa funcao so ira parsear se o SQL for identificavel.```sql\n from table\n```")



def main():
    st.set_page_config(layout="wide")
    # st.sidebar.header("Camp Kit ")
    st.sidebar.subheader("*Composable* DATA Stackpack ")
    st.sidebar.text("Sidebar content goes here.")

    menu = ["project", "polyglot", "void"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "project":
        st.subheader("The project")
        t = """
        Traditional data management systems have grown organically, leading to a fragmented landscape of monolithic products with limited interoperability. This has resulted in high maintenance costs, slowed innovation, and a steep learning curve for developers and end users alike, who must navigate various incompatible SQL and non-SQL APIs. The need for a more modular approach to data management has become evident, one that can leverage the latest open-source technologies to create a unified, consistent data ecosystem.
        """
        st.write(t)

    elif choice == "polyglot":
        st.image(r"app/images/logo-bg.png", caption="Convert SQL Dialects easily.", width=350 )

        button_convert = st.button("Run", type="primary")
        col1, col2 = st.columns(2)
        container1 = col1.container(border=True)
        from_dialect = container1.selectbox("**From** Language:", DIALECTS, index=None,
                                            placeholder="Select")

        to_query = container1.text_area(
            label=f"Spell the code.",
            height=1024,
            max_chars=50000,
            placeholder="Only SQL statements"
        )

        container2 = col2.container(border=True)
        to_dialect = container2.selectbox("**To** Language:", DIALECTS, index=None,
                                          placeholder="Select")

        try:
            if button_convert:
                if not to_query:
                    parser_error(error='vam la coloca a query ai ai')
                else:
                    if not to_dialect or not from_dialect:
                        parser_error(error='vam la escolhe o dialeto ai')
                    else:
                        compilide_query = beauty_transpiler(query=to_query, read=from_dialect, write=to_dialect)
                        container2.markdown("*The code spell.*")
                        container2.write_stream(streamer("```sql\n" + compilide_query + "\n```"),)
        except Exception as err:
            parser_error(error=err)
    elif choice == "void":
        sql_analysis_msg()
        st.subheader("void")
        button_get = st.button("Run", type="primary")
        container1 = st.container(border=True)
        # button_get = container1.button("Run", type="primary",align="rigth")
        from_dialect = container1.selectbox("**From** Language:", DIALECTS, index=None,
                                            placeholder="Select")

        source_query = container1.text_area(
            label=f"Spell the code.",
            height=300,
            max_chars=50000,
            placeholder="Only SQL statements"
        )

        container2 = st.container(border=True)
        tab1, tab2, tab3, tab4, tab5 = container2.tabs(["Table", "JSON", "Analytics", "Network", "AI"])

        if button_get:
            try:
                df = get_lineage_df(source_query, from_dialect)
                with tab1: #table
                    st.write(df)
                with tab2: #json
                    stmt = json.loads(df.to_json())
                    st.write(stmt)
                with tab3: #analytics
                    container0 = st.container(border=True)

                    container0.write("How many objects were parsed by statements in my query?")
                    stmt_hits = df.groupby(['StatementIndex']).size().reset_index(name='ParsedObjects')
                    container0.bar_chart(stmt_hits, x='StatementIndex', y='ParsedObjects')

                    analysis = get_analysis_df(sql_query=source_query,dialect=from_dialect)

                    st.write(analysis)

                    col1, col2 = st.columns([2, 2])

                    container1a = col1.container(border=True)

                    container1a.write("Which columns have the most hits?")
                    column_hits = df.groupby(['FullyIdentifierTableName', 'ColumnName']).size().reset_index(
                        name='ColumnHits')
                    container1a.write(column_hits.sort_values(by='ColumnHits', ascending=False))

                    with col1:
                        container1 = col1.container(border=True)


                        # pie chart
                        container1.write("Which schema have the most hits?")
                        schema_hits = df.groupby(['SchemaName']).size().reset_index(name='Hits')
                        fig = px.pie(schema_hits, values='Hits', names='SchemaName')
                        container1.plotly_chart(fig)


                    with col2:
                        container2 = col2.container(border=True)
                        container2a = col2.container(border=True)
                        # bar char
                        table_counts = df['FullyIdentifierTableName'].value_counts().reset_index()
                        table_counts.columns = ['FullyIdentifierTableName', 'Count']

                        # Calcular o percentual em relação ao total geral
                        total_count = df.shape[0]
                        table_counts['Percentage'] = (table_counts['Count'] / total_count) * 100
                        table_counts = table_counts.sort_values(by='Percentage', ascending=False)

                        container2.write("Distribution of dependencies by tables.")
                        # container2.write("*X-axis represents the tables, Y-axis is the number of references located in the query and the color is the scale in percentage*")
                        container2.bar_chart(table_counts, x='FullyIdentifierTableName', y='Count', color='Percentage', horizontal=True)

                        container2a.write("Which tables does my query depend on the most?")
                        container2a.dataframe(table_counts)

                     # st.bar_chart(df, x="PhysicalTableName", y="ColumnName", horizontal=True)

            except Exception as err:
                parser_error(error=err)
    else:
        st.subheader("About")


if __name__ == '__main__':
    main()
