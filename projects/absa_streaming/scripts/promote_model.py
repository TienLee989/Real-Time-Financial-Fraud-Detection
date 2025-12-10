import os, shutil, pandas as pd

CURR_PT = "/opt/airflow/projects/absa_streaming/models/current/best_absa_hardshare.pt"
CURR_MET = "/opt/airflow/projects/absa_streaming/models/current/metrics.csv"
CAND_DIR = "/opt/airflow/projects/absa_streaming/models/candidates"

def current_f1():
    if os.path.exists(CURR_MET):
        try:
            return float(pd.read_csv(CURR_MET).iloc[0]["f1_macro"])
        except Exception:
            return 0.0
    return 0.0

metrics_files = sorted([f for f in os.listdir(CAND_DIR) if f.endswith(".metrics.csv")])
if not metrics_files:
    print("[Promote] ❌ No candidate metrics found.")
    raise SystemExit(0)

latest = metrics_files[-1]
cand_metrics = pd.read_csv(os.path.join(CAND_DIR, latest)).iloc[0]
new_f1 = float(cand_metrics["f1_macro"])
old_f1 = current_f1()

if new_f1 > old_f1:
    src_pt = os.path.join(CAND_DIR, cand_metrics["model_id"])
    shutil.copyfile(src_pt, CURR_PT)
    cand_metrics.to_frame().T.to_csv(CURR_MET, index=False)
    print(f"[Promote] ✅ Promoted {cand_metrics['model_id']} (f1={new_f1:.3f} > {old_f1:.3f})")
else:
    print(f"[Promote] ⏭️ Skipped (new={new_f1:.3f} <= old={old_f1:.3f})")
