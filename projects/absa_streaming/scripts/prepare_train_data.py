import os, pandas as pd

IN_CSV = "/opt/airflow/projects/absa_streaming/data/test_data.csv"
OUT_CSV = "/opt/airflow/projects/absa_streaming/models/candidates/train_long.csv"
ASPECTS = ["Price","Shipping","Outlook","Quality","Size","Shop_Service","General","Others"]
POSITIVE_VALUES = {1, 2}

print("[PrepareTrain] 🔧 Preparing long-format training data...")
df = pd.read_csv(IN_CSV)

rows = []
for _, r in df.iterrows():
    text = str(r["Review"])
    for asp in ASPECTS:
        val = r.get(asp, 0)
        if val in POSITIVE_VALUES:
            label = "positive"
        elif val == -1:
            label = "negative"
        else:
            label = "neutral"
        rows.append({"review": text, "aspect": asp, "label": label})

out = pd.DataFrame(rows)
os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
out.to_csv(OUT_CSV, index=False)
print(f"[PrepareTrain] ✅ Saved {len(out)} rows -> {OUT_CSV}")
