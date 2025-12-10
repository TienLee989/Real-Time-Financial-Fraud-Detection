#!/usr/bin/env bash
# ===========================================================
# 🚀 install_requirements.sh
# Mục đích: Copy và cài đặt các package Python vào container Airflow
# ===========================================================

# Đường dẫn đến file requirements.txt trên máy host
REQ_FILE="base/requirements.txt"

# Danh sách các container Airflow cần cài
CONTAINERS=(
  airflow_webserver
  airflow_scheduler
  airflow_worker
  airflow_triggerer
)

echo "=============================================="
echo "📦 Bắt đầu cài đặt requirements cho các container Airflow"
echo "=============================================="
echo

# Kiểm tra file requirements.txt có tồn tại không
if [ ! -f "$REQ_FILE" ]; then
  echo "❌ Không tìm thấy file $REQ_FILE"
  exit 1
fi

# Lặp qua từng container để copy và cài đặt
for c in "${CONTAINERS[@]}"; do
  echo "➡️  Đang xử lý container: $c"

  # Kiểm tra container có đang chạy không
  if docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
    echo "   📁 Copying requirements.txt vào container..."
    docker cp "$REQ_FILE" "$c:/opt/airflow/requirements.txt"

    echo "   ⚙️  Đang cài đặt packages..."
    docker exec "$c" pip install --no-cache-dir -r /opt/airflow/requirements.txt

    echo "   ✅ Hoàn tất cài đặt trong $c"
  else
    echo "   ⚠️  Container $c không chạy, bỏ qua."
  fi
  echo
done

echo "=============================================="
echo "🎉 Hoàn tất cài đặt requirements cho tất cả container!"
echo "=============================================="
