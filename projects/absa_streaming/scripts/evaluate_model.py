import os, pandas as pd, numpy as np

CAND_DIR = "/opt/airflow/projects/absa_streaming/models/candidates"

# Lấy candidate mới nhất
cands = sorted([f for f in os.listdir(CAND_DIR) if f.endswith(".pt")])
if not cands:
    print("[Evaluate] ❌ No candidate found.")
    raise SystemExit(0)
cand = cands[-1]

# Demo: random metrics trong khoảng hợp lý
acc = float(np.random.uniform(0.86, 0.94))
f1  = float(np.random.uniform(0.85, 0.93))

m = pd.DataFrame([{
    "model_id": cand,
    "accuracy": round(acc, 3),
    "f1_macro": round(f1, 3),
    "created_at": pd.Timestamp.now()
}])
m_csv = os.path.join(CAND_DIR, f"{cand}.metrics.csv")
m.to_csv(m_csv, index=False)
print(f"[Evaluate] ✅ {cand} -> acc={acc:.3f}, f1={f1:.3f} (saved {m_csv})")
