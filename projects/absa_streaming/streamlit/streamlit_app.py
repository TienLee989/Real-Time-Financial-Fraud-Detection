import os, pandas as pd, streamlit as st, plotly.express as px
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

DB_USER = os.getenv("DB_USER","airflow")
DB_PWD  = os.getenv("DB_PWD","airflow")
DB_HOST = os.getenv("DB_HOST","postgres")
DB_PORT = int(os.getenv("DB_PORT","5432"))
DB_NAME = os.getenv("DB_NAME","absa_db")

ENGINE = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

st.set_page_config(page_title="💬 ABSA Streaming Dashboard", page_icon="🧠", layout="wide")

@st.cache_data(ttl=5)
def load_df(sql: str):
    """
    Đọc dữ liệu an toàn với SQLAlchemy 2.0 + psycopg2.
    - Dùng connection.connection để lấy raw DB-API connection thật.
    - Dùng pd.read_sql an toàn.
    """
    with ENGINE.connect() as connection:
        raw_conn = connection.connection
        df = pd.read_sql(sql, raw_conn)
        return df

st.markdown("""
<div style="text-align:center; margin-bottom: 8px;">
  <h1 style="color:#1F4172;">💬 Real-time ABSA Sentiment Analysis</h1>
  <p style="color:gray">Kafka × Spark × PostgreSQL × Streamlit × Airflow</p>
  <p><b>Người thực hiện:</b> <span style="color:#005C99;">Lê Văn Tiến – MSHV: 240201027</span></p>
</div>
""", unsafe_allow_html=True)

st_autorefresh(interval=5000, key="absa_autorefresh")

# ====== Data ======
try:
    df = load_df("""
        SELECT id, review, aspect, sentiment, confidence, model_id, processed_at
        FROM absa_results
        ORDER BY processed_at DESC
        LIMIT 2000
    """)
except Exception as e:
    st.error(f"❌ Không thể kết nối tới PostgreSQL: {e}")
    st.stop()

if df.empty:
    st.warning("⏳ Chưa có dữ liệu trong `absa_results`…")
    st.stop()

# ====== Overview ======
st.markdown("### 📊 Tổng quan")
total = len(df)
pos = (df["sentiment"]=="positive").sum()
neu = (df["sentiment"]=="neutral").sum()
neg = (df["sentiment"]=="negative").sum()
c1,c2,c3,c4 = st.columns(4)
c1.metric("Tổng dòng (review×aspect)", f"{total:,}")
c2.metric("Tích cực", pos)
c3.metric("Trung tính", neu)
c4.metric("Tiêu cực", neg)

st.divider()

# ====== Charts ======
left, right = st.columns(2)
with left:
    pie = px.pie(
        df, names="sentiment", title="Tỉ lệ cảm xúc (toàn bộ)",
        color="sentiment",
        color_discrete_map={"positive":"#2ecc71","neutral":"#f1c40f","negative":"#e74c3c"},
        hole=0.4
    )
    pie.update_layout(title_x=0.5)
    st.plotly_chart(pie, use_container_width=True)

with right:
    df_time = (df.assign(minute=pd.to_datetime(df["processed_at"]).dt.floor("min"))
                 .groupby(["minute","sentiment"]).size()
                 .reset_index(name="count"))
    line = px.line(df_time, x="minute", y="count", color="sentiment",
                   markers=True, title="Xu hướng theo thời gian")
    line.update_layout(title_x=0.5)
    st.plotly_chart(line, use_container_width=True)

st.divider()

# ====== By Aspect ======
st.markdown("### 🧭 Phân tích theo Aspect")
by_asp = df.groupby(["aspect","sentiment"]).size().reset_index(name="count")
bar = px.bar(by_asp, x="aspect", y="count", color="sentiment", barmode="group",
             title="Phân bổ cảm xúc theo từng Aspect",
             color_discrete_map={"positive":"#2ecc71","neutral":"#f1c40f","negative":"#e74c3c"})
bar.update_layout(title_x=0.5)
st.plotly_chart(bar, use_container_width=True)

# ====== Latest Table ======
st.markdown("### 🧾 Dòng ghi nhận gần đây")
st.dataframe(df.head(30), use_container_width=True, hide_index=True)

st.divider()

# ====== Model Registry ======
st.markdown("### 🤖 Model Registry & Metrics")
# Current metrics (file)
curr_metrics_path = "/opt/airflow/projects/absa_streaming/models/current/metrics.csv"
curr_df = None
if os.path.exists(curr_metrics_path):
    try:
        curr_df = pd.read_csv(curr_metrics_path)
    except Exception:
        curr_df = None

colL, colR = st.columns([1,1])
with colL:
    st.subheader("Model hiện tại (filesystem)")
    st.write(f"**File:** `/models/current/best_absa_hardshare.pt`")
    if curr_df is not None and not curr_df.empty:
        st.dataframe(curr_df, use_container_width=True, hide_index=True)
    else:
        st.info("Chưa có metrics cho model hiện tại.")

with colR:
    st.subheader("Registry (database)")
    try:
        reg = load_df("""
            SELECT r.model_id, r.path, r.is_active,
                   COALESCE(m.accuracy,0) AS accuracy,
                   COALESCE(m.f1_macro,0) AS f1_macro,
                   r.created_at
            FROM absa_model_registry r
            LEFT JOIN absa_model_metrics m ON r.model_id = m.model_id
            ORDER BY r.created_at DESC
        """)
        st.dataframe(reg, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Không load được registry: {e}")

st.markdown("""
<div style="text-align:center; color:gray; margin-top:1rem;">
  © 2025 – Real-time ABSA Streaming System | Kafka × Spark × PostgreSQL × Streamlit × Airflow
</div>
""", unsafe_allow_html=True)
