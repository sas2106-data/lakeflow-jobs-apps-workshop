"""Workshop — Streamlit hello world on Databricks Apps (+ stretch: read the job's gold table)."""
import os

import streamlit as st

st.set_page_config(page_title="Claims hello", page_icon="👋", layout="wide")

# --- Module 4: hello world ----------------------------------------------------
st.title("👋 Hello from Databricks Apps")
st.caption("Lakeflow Jobs & Databricks Apps workshop")

# Databricks Apps forwards the signed-in user's identity in request headers
try:
    headers = st.context.headers
except AttributeError:  # older Streamlit
    headers = {}
user = headers.get("X-Forwarded-Preferred-Username") or headers.get("X-Forwarded-Email") or "there"
name = st.text_input("Your name", value=user.split("@")[0])
if st.button("Say hello"):
    st.success(f"Hello, {name}! This app is running on serverless compute in your workspace.")

# --- Stretch: show the gold table written by the claims_medallion job ---------
st.divider()
st.subheader("Gold claims KPIs (written by the Lakeflow Job)")

warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
table = st.text_input("Gold table", value=os.getenv("SUMMARY_TABLE", ""))
if not warehouse_id:
    st.info("Add a SQL warehouse resource named `sql-warehouse` to the app to enable this section.")
    st.stop()


@st.cache_data(ttl=60)
def load_summary(tbl: str):
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config()  # uses the app's service principal credentials
    with sql.connect(server_hostname=cfg.host,
                     http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                     credentials_provider=lambda: cfg.authenticate) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {tbl} ORDER BY line_of_business")
            return cur.fetchall_arrow().to_pandas()


try:
    df = load_summary(table)
    c1, c2, c3 = st.columns(3)
    c1.metric("Claims", f'{int(df["claims"].sum()):,}')
    c2.metric("Open claims", f'{int(df["open_claims"].sum()):,}')
    c3.metric("Outstanding", f'{float(df["outstanding_amount"].sum()):,.0f}')
    st.bar_chart(df, x="line_of_business", y="open_claims")
    st.dataframe(df, hide_index=True, use_container_width=True)
except Exception as e:  # most common cause: the app's service principal lacks SELECT on the table
    st.warning(f"Couldn't read `{table}`. Grant the app's service principal USE CATALOG, USE SCHEMA "
               f"and SELECT (Module 5).\n\n`{type(e).__name__}: {str(e)[:300]}`")
