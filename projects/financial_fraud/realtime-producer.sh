#!/usr/bin/env bash
#
# Real-time Fraud Producer (push 1 record every 3 seconds)
#

SCRIPT_DIR=""
PRODUCER_PY="$SCRIPT_DIR/financial_fraud_producer_realtime.py"

echo "====================================================="
echo "🚀 Starting Real-time Financial Fraud Producer"
echo "➡ Push 1 record every 3 seconds"
echo "====================================================="

PYTHON_BIN="/usr/bin/python3"

# Check file
if [ ! -f "$PRODUCER_PY" ]; then
  echo "❌ ERROR: Producer script not found: $PRODUCER_PY"
  exit 1
fi

while true; do
    echo ""
    echo "➡ Sending 1 new real-time fraud event..."
    $PYTHON_BIN "$PRODUCER_PY"

    echo "⏳ Waiting 3 seconds..."
    sleep 3
done
