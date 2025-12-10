import streamlit as st
import pandas as pd
import plotly.express as px
import json
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

# ============================================================
# STREAMLIT CONFIG
# ============================================================
st.set_page_config(
    page_title="💳 Financial Fraud Monitoring",
    page_icon="🕵️‍♂️",
    layout="wide",
)

# ============================================================
# DATABASE CONFIG (PostgreSQL - fraud_db)
# ============================================================
DB_STR = "postgresql+psycopg2://airflow:airflow@postgres:5432/fraud_db"
engine = create_engine(DB_STR, pool_pre_ping=True)

# ============================================================
# RULES CONFIG (Load Rule Descriptions)
# ============================================================
# NOTE: Trong môi trường thực tế, file này cần được mount vào Streamlit container
try:
    # Vẫn giữ đường dẫn tuyệt đối đã được thiết lập
    with open("/opt/airflow/projects/financial_fraud/data/rules.json", encoding="utf-8") as f:
        RULE_DEFINITIONS = json.load(f)
except FileNotFoundError:
    RULE_DEFINITIONS = {}
    st.error("❌ Không tìm thấy file rules.json. Vui lòng kiểm tra đường dẫn.")


# ============================================================
# UTILS
# ============================================================
@st.cache_data(ttl=4)
def load_sql(sql):
    try:
        with engine.connect() as conn:
            raw = conn.connection
            return pd.read_sql(sql, raw)
    except Exception as e:
        # Giữ nguyên lỗi PostgreSQL khi load SQL
        st.error(f"❌ ERROR PostgreSQL: {e}")
        return pd.DataFrame()

# AUTO REFRESH
st_autorefresh(interval=5000, key="refresh")

# ============================================================
# CUSTOM DISPLAY FUNCTION
# ============================================================
def display_rule_details(df_features, df_prediction, df_violations):
    """Hiển thị chi tiết kết quả Inference và Rule Violation."""
    
    if df_prediction.empty or df_features.empty:
        st.warning("Không tìm thấy dữ liệu chi tiết.")
        return

    # Lấy dữ liệu chính
    pred_data = df_prediction.iloc[0]
    features_raw = json.loads(df_features["features"].iloc[0])
    
    fraud_score = pred_data['fraud_score']
    prediction = pred_data['prediction']
    
    # 1. PHÂN TÍCH VÀ HIỂN THỊ ĐIỂM DỰ ĐOÁN
    if fraud_score >= 0.8:
        level = "HIGH"
        color = "red"
        st.error(f"🔥 CẢNH BÁO MỨC {level}: Gian lận xác suất cao!", icon="🚨")
    elif fraud_score >= 0.5:
        level = "MEDIUM"
        color = "orange"
        st.warning(f"⚠️ CẢNH BÁO MỨC {level}: Nghi ngờ gian lận.", icon="⚠️")
    else:
        level = "LOW"
        color = "green"
        st.success(f"✅ Giao dịch an toàn. Mức rủi ro {level}.", icon="✅")

    st.markdown(f"**Fraud Score (Model)**: <span style='color:{color}; font-size: 1.5rem;'>**{fraud_score:.4f}**</span> (Prediction: {'FRAUD' if prediction == 1 else 'SAFE'})", unsafe_allow_html=True)
    
    st.markdown("---")

    # 2. PHÂN TÍCH VÀ HIỂN THỊ RULE ENGINE
    if not df_violations.empty:
        st.subheader("⚠️ Quy tắc Gian lận bị vi phạm")
        st.markdown(f"**Tổng số Rules Vi phạm**: **{len(df_violations)}**")
        
        for idx, row in df_violations.iterrows():
            rule_id = row['rule_id']
            feature = row['feature']
            value = row['value']
            note = row['note']
            
            st.markdown(f"""
            <div style="padding: 10px; border-left: 5px solid #ff4b4b; margin-bottom: 10px; background-color: rgb(255 242 242); border-radius: 4px;">
                <strong>🚨 {rule_id}</strong>: {note} <br>
                <small><i>Feature: {feature}, Giá trị: <b>{value}</b></i></small>
            </div>
            """, unsafe_allow_html=True)

    else:
        st.info("Không có Quy tắc Gian lận nào bị vi phạm trực tiếp.")
        
    st.markdown("---")
    
    # 3. CHI TIẾT ĐẦU VÀO (RAW FEATURES)
    with st.expander("📦 Chi tiết các Trường dữ liệu đầu vào", expanded=False):
        # Hiển thị features dưới dạng bảng chuyên nghiệp hơn
        features_df = pd.DataFrame(features_raw, index=[0]).T.reset_index()
        features_df.columns = ['Feature', 'Value']
        
        st.dataframe(features_df, hide_index=True, use_container_width=True)


# ============================================================
# HEADER
# ============================================================
st.markdown("""
<div style="text-align:center; margin-bottom: 1rem;">
    <h1 style="color:#b30000;">💳 Real-Time Financial Fraud Detection Dashboard</h1>
    <p style="color:#555; font-size:1.1rem;">
        Kafka → Spark → TensorFlow → Rule Engine → PostgreSQL → Streamlit
    </p>
    <p><b>Người thực hiện:</b>
    <span style="color:#b30000;">Lê Văn Tiến – MSHV: 240201027</span></p>
</div>
""", unsafe_allow_html=True)

# ============================================================
# LOAD CORE DATA
# ============================================================
# Tải dữ liệu prediction
df_pred_raw = load_sql("""
    SELECT transaction_id, fraud_score, prediction, violated_rule_count, processed_at
    FROM fraud_predictions
    ORDER BY processed_at DESC
    LIMIT 500
""")

if df_pred_raw.empty:
    st.warning("⏳ No predictions yet. Start Kafka producer + Spark consumer.")
    st.stop()

df_pred_raw["processed_at"] = pd.to_datetime(df_pred_raw["processed_at"])

# Tạo DataFrame chính (df_pred) từ df_pred_raw để tính toán các metrics
df_pred = df_pred_raw.copy()

# ============================================================
# TOP METRICS
# ============================================================
st.markdown("### 📊 Fraud Summary")

col1, col2, col3, col4, col5 = st.columns(5)

total_tx = df_pred.shape[0]
fraud_tx = df_pred[df_pred.prediction == 1].shape[0]
fraud_rate = fraud_tx / max(total_tx, 1) * 100
avg_score = df_pred["fraud_score"].mean()

# Loại bỏ múi giờ khỏi giá trị so sánh để tránh TypeError
time_threshold = (pd.Timestamp.utcnow() - pd.Timedelta(minutes=5)).tz_localize(None)

last_min = df_pred[df_pred["processed_at"] > time_threshold]
recent_fraud = last_min[last_min["prediction"] == 1].shape[0]

col1.metric("📦 Tổng giao dịch", f"{total_tx:,}")
col2.metric("🚨 Tổng gian lận", f"{fraud_tx:,}")
col3.metric("🔥 Tỷ lệ gian lận", f"{fraud_rate:.2f}%")
col4.metric("📊 Fraud Score TB", f"{avg_score:.3f}")
col5.metric("⏱️ Fraud 5 phút gần nhất", recent_fraud)

st.divider()

# ============================================================
# VISUAL ANALYTICS
# ============================================================
colA, colB = st.columns(2)

# Histogram Fraud Score
with colA:
    fig = px.histogram(
        df_pred,
        x="fraud_score",
        nbins=30,
        color="prediction",
        title="📈 Phân bố Fraud Score",
        color_discrete_map={0: "green", 1: "red"},
    )
    fig.update_layout(title_x=0.5)
    st.plotly_chart(fig, use_container_width=True)

# Fraud Over Time
with colB:
    df_time = (
        df_pred.groupby(df_pred["processed_at"].dt.floor("min"))["prediction"]
        .sum()
        .reset_index()
    )
    fig2 = px.line(
        df_time,
        x="processed_at",
        y="prediction",
        title="⏱️ Gian lận theo thời gian (per minute)",
        markers=True,
        color_discrete_sequence=["red"],
    )
    fig2.update_layout(title_x=0.5)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ============================================================
# RULES ANALYTICS
# ============================================================
st.markdown("### 🧩 Fraud Rules Analytics")

df_rules_agg = load_sql("""
    SELECT rule_id, feature, note, COUNT(*) as count
    FROM fraud_rules_log
    GROUP BY rule_id, feature, note
    ORDER BY count DESC
    LIMIT 10
""")

if not df_rules_agg.empty:
    fig_rules = px.bar(
        df_rules_agg,
        x="rule_id",
        y="count",
        color="feature",
        text_auto=True,
        title="🔥 Top Violated Rules",
    )
    fig_rules.update_layout(title_x=0.5)
    st.plotly_chart(fig_rules, use_container_width=True)
else:
    st.info("Chưa có rule nào được kích hoạt.")

st.divider()

# ============================================================
# ALERT BOX (fraud_alerts)
# ============================================================
st.markdown("### 🚨 Fraud Alerts (Realtime)")

df_alerts = load_sql("""
    SELECT *
    FROM fraud_alerts
    ORDER BY created_at DESC
    LIMIT 10
""")

if df_alerts.empty:
    st.success("✔ Không có cảnh báo fraud mức cao.")
else:
    st.error("🔥 High-risk alerts detected!")
    st.dataframe(df_alerts, hide_index=True, use_container_width=True)

st.divider()

# ============================================================
# TRANSACTION DETAIL / SEARCH (Nâng cấp)
# ============================================================
st.markdown("### 🔍 Tra cứu chi tiết giao dịch")

search_id = st.text_input("Nhập Transaction ID cần tra cứu")

if search_id:
    # 1. Tải Raw Features
    df_features = load_sql(f"""
        SELECT features
        FROM fraud_feature_store
        WHERE transaction_id = '{search_id}'
    """)
    
    # 2. Tải Prediction Result
    df_prediction = load_sql(f"""
        SELECT transaction_id, fraud_score, prediction, violated_rule_count, processed_at
        FROM fraud_predictions
        WHERE transaction_id = '{search_id}'
    """)
    
    # 3. Tải Rule Violations
    df_violations = load_sql(f"""
        SELECT rule_id, feature, value, note, triggered_at
        FROM fraud_rules_log
        WHERE transaction_id = '{search_id}'
        ORDER BY triggered_at DESC
    """)


    if df_features.empty:
        st.warning(f"❌ Không tìm thấy dữ liệu cho Transaction ID: **{search_id}**.")
    else:
        st.subheader(f"Giao dịch: {search_id}")
        
        # Gọi hàm hiển thị chi tiết
        display_rule_details(df_features, df_prediction, df_violations)

st.divider()

# ============================================================
# RECENT TRANSACTIONS (Nâng cấp hiển thị)
# ============================================================
st.markdown("### 🧾 Giao dịch gần đây")

# 1. Tạo cột Risk Level và Format hiển thị
df_display = df_pred.copy()

# Map prediction sang trạng thái text
df_display['Trạng thái'] = df_display['prediction'].map({1: '🚨 FRAUD', 0: '✅ SAFE'})

# Tính toán cột Risk Level dựa trên Fraud Score
def get_risk_level(score):
    if score >= 0.8:
        return '🔥 HIGH'
    elif score >= 0.5:
        return '⚠️ MEDIUM'
    else:
        return 'LOW'

df_display['Mức độ Rủi ro'] = df_display['fraud_score'].apply(get_risk_level)

# Format cột thời gian
df_display['Thời gian Xử lý'] = df_display['processed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Chọn và sắp xếp lại các cột để hiển thị chuyên nghiệp hơn
df_display = df_display[[
    'Thời gian Xử lý',
    'transaction_id',
    'fraud_score',
    'Mức độ Rủi ro',
    'Trạng thái',
    'violated_rule_count'
]]

# Đổi tên cột
df_display.columns = [
    'Thời gian Xử lý',
    'Transaction ID',
    'Fraud Score',
    'Mức độ Rủi ro',
    'Trạng thái',
    'Rules Vi phạm'
]

# Đặt màu sắc cho cột 'Trạng thái'
def highlight_status(val):
    if val == '🚨 FRAUD':
        color = 'background-color: #ff4b4b; color: white'
    elif val == '✅ SAFE':
        color = 'background-color: #00cc00; color: white'
    else:
        color = ''
    return color

st.dataframe(
    df_display.head(30).style.applymap(
        lambda x: 'color: red' if x == '🔥 HIGH' else ('color: orange' if x == '⚠️ MEDIUM' else ''),
        subset=['Mức độ Rủi ro']
    ).applymap(
        highlight_status,
        subset=['Trạng thái']
    ),
    hide_index=True,
    use_container_width=True,
    column_config={
        'Fraud Score': st.column_config.ProgressColumn(
            "Fraud Score",
            help="Điểm rủi ro gian lận",
            format="%.4f",
            min_value=0,
            max_value=1,
            width='small'
        )
    }
)

st.markdown("""
<div style="text-align:center; color:gray; margin-top:2rem;">
    <p>© 2025 – Financial Fraud Monitoring System</p>
</div>
""", unsafe_allow_html=True)