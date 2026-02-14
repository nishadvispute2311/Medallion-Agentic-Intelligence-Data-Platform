import streamlit as st
from databricks import sql
import os

# ------------------------------------------
# ENABLE LOGGING
# ------------------------------------------

from databricks.sdk import WorkspaceClient
import mlflow
from openai import OpenAI

DATABRICKS_SERVER_HOST_NAME=""
DATABRICKS_SQL_HTTP_PATH=""
DBX_ACCESS_TOKEN=""


openai_client = OpenAI(
    api_key=DBX_ACCESS_TOKEN,
    base_url=f"https://{DATABRICKS_SERVER_HOST_NAME}/serving-endpoints"
)

# ------------------------------------
# Databricks SQL Connection
# ------------------------------------
connection = sql.connect(
    server_hostname=DATABRICKS_SERVER_HOST_NAME,
    http_path=DATABRICKS_SQL_HTTP_PATH,
    access_token=DBX_ACCESS_TOKEN
)

# ------------------------------------
# Fetch business insights
# ------------------------------------
def fetch_recent_insights(limit=14):
    query = f"""
        SELECT insight_date, net_revenue, net_revenue_wow_pct, orders_count, orders_wow_pct, payment_success_rate, payment_health, refund_rate, refund_risk, discount_pct, discount_health, revenue_driver, executive_summary, risk_flag
        FROM agentic_ai_catalog.agent_test.agent_business_daily_insights
        ORDER BY insight_date DESC
        LIMIT {limit}
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return rows

def call_llm(prompt):
    response = openai_client.chat.completions.create(
        model="LLM_NAME",
        messages=[
            {"role": "system", "content": "You are a senior business analyst."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )
    return response.choices[0].message.content

# ------------------------------------
# LLM Call
# ------------------------------------
def ask_business_bot(question):

    rows = fetch_recent_insights()

    context_text = "\n".join([
        f"Date: {r[0]}, Net Revenue: {r[1]}, Net Revenue Week-over-week percent: {r[2]}, Orders Count: {r[3]}, Orders week-over-week percent: {r[4]}, Payment Success Rate: {r[5]}, Payment Health : {r[6]}, Refund Rate: {r[7]}, Refund Risk: {r[8]}, Discount Percent: {r[9]}, Driver: {r[11]}, Risk: {r[13]}\nSummary: {r[12]}"
        for r in rows
    ])

    prompt = f"""
You are a Business Insight Assistant for senior leadership.

Answer the user question using ONLY the context below.
Do not invent data but try to answer the aggregated questions like are we profitable in current year or not? or there is any risk in our business? like these questions based on the results you have try to think and answer the question.
Context:
{context_text}

User Question:
{question}

Respond concisely and clearly in 3-4 lines.
"""

    llm_result = call_llm(prompt)

    return llm_result


# ------------------------------------
# Streamlit UI
# ------------------------------------
st.set_page_config(page_title="Business Insight Chatbot", layout="wide")
st.title("📊 Business Insight Chatbot")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

user_input = st.chat_input("Ask a business question...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing business insights..."):
            answer = ask_business_bot(user_input)
            st.markdown(answer)

    st.session_state.messages.append({"role": "assistant", "content": answer})
