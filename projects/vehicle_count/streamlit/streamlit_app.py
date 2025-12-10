import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
from datetime import datetime

# ============================================================
# ⚙️ Cấu hình cơ bản
# ============================================================
st.set_page_config(
    page_title="🚗 Vehicle Streaming Dashboard",
    page_icon="🚘",
    layout="wide",
)

DB_CONFIG = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
    "database": "vehicle_db"
}

# ============================================================
# 🧠 Hàm đọc dữ liệu từ PostgreSQL
# ============================================================
@st.cache_data(ttl=5)
def load_data():
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    try:
        with engine.connect() as conn:  # ✅ Dùng connect() thay vì raw_connection()
            df = pd.read_sql(
                """
                SELECT * FROM vehicle_counts
                ORDER BY processed_at DESC
                LIMIT 500
                """,
                conn
            )
        return df
    except Exception as e:
        st.error(f"❌ Lỗi kết nối đến PostgreSQL: {e}")
        return pd.DataFrame()
    
@st.cache_data(ttl=5)
def load_data():
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    try:
        conn = engine.raw_connection()
        df = pd.read_sql("SELECT * FROM vehicle_counts ORDER BY processed_at DESC LIMIT 300", conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"⚠️ Cannot connect PostgreSQL: {e}")
        return pd.DataFrame()
# ============================================================
# 🏁 Giao diện Dashboard
# ============================================================

# Header
st.markdown("""
<div style="text-align:center; margin-bottom: 1rem;">
    <h1 style="color:#1F4172;">🚗 Real-time Vehicle Counting Dashboard</h1>
    <p style="color:gray; font-size:1.1rem;">
        Kafka → Spark → PostgreSQL → Streamlit
    </p>
    <p><b>Người thực hiện:</b> <span style="color:#005C99;">Lê Văn Tiến – MSHV: 240201027</span></p>
</div>
""", unsafe_allow_html=True)

# Tự động refresh 5s/lần
st_autorefresh(interval=5000, limit=None, key="auto_refresh")

# Load data
df = load_data()

# ============================================================
# 📊 Hiển thị dữ liệu
# ============================================================
if df.empty:
    st.warning("⏳ Chưa có dữ liệu trong bảng `vehicle_counts`. Hãy đảm bảo Kafka producer và Spark consumer đang chạy.")
else:
    # Tổng quan
    st.markdown("### 📊 Tổng quan dữ liệu")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📦 Tổng số xe", f"{int(df['count'].sum()):,}")
    col2.metric("📷 Số camera hoạt động", df['camera_id'].nunique())
    col3.metric("🚘 Số loại phương tiện", df['vehicle_type'].nunique())
    latest_time = pd.to_datetime(df['processed_at']).max()
    col4.metric("🕒 Cập nhật gần nhất", latest_time.strftime("%H:%M:%S"))

    st.divider()

    # Biểu đồ
    st.markdown("### 📈 Phân tích thống kê phương tiện")

    colA, colB = st.columns(2)

    with colA:
        df_bar = (
            df.groupby(["camera_id", "vehicle_type"])["count"]
            .sum()
            .reset_index()
        )
        fig_bar = px.bar(
            df_bar,
            x="camera_id",
            y="count",
            color="vehicle_type",
            text_auto=True,
            barmode="group",
            title="Số lượng phương tiện theo Camera & Loại xe",
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig_bar.update_layout(
            xaxis_title="Camera ID",
            yaxis_title="Số lượng xe",
            title_x=0.5
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with colB:
        df_line = (
            df.groupby(["frame_time", "vehicle_type"])["count"]
            .sum()
            .reset_index()
        )
        fig_line = px.line(
            df_line,
            x="frame_time",
            y="count",
            color="vehicle_type",
            markers=True,
            title="Xu hướng đếm xe theo thời gian (Real-time)",
            color_discrete_sequence=px.colors.qualitative.Vivid
        )
        fig_line.update_layout(
            xaxis_title="Thời gian khung hình",
            yaxis_title="Số lượng xe",
            title_x=0.5
        )
        st.plotly_chart(fig_line, use_container_width=True)

    st.divider()

    # Dữ liệu gần đây
    st.markdown("### 🧾 Dữ liệu ghi nhận gần đây")
    st.dataframe(
        df.head(20),
        use_container_width=True,
        hide_index=True,
    )

    # Footer
    st.markdown("""
    <div style="text-align:center; color:gray; margin-top:2rem;">
        <p>© 2025 – Hệ thống Giám sát Giao thông Thông minh | Kafka × Spark × Streamlit</p>
    </div>
    """, unsafe_allow_html=True)
