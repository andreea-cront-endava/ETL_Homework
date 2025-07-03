import pandas as pd
import oracledb
from datetime import timedelta

# === 1. Conectare la Oracle ===
conn = oracledb.connect(user="sources", password="sources123", dsn="localhost:1521/XEPDB1")

query = """
SELECT emp_name, session_name, start_time, end_time
FROM attendance
ORDER BY emp_name, session_name, start_time
"""

df = pd.read_sql(query, conn)
conn.close()

# === 2. Funcție pentru a uni intervale și calcula durata reală ===
def total_active_minutes(group):
    intervals = list(zip(group['start_time'], group['end_time']))
    intervals.sort()

    merged = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)

    total = sum((end - start for start, end in merged), timedelta())
    return total.total_seconds() // 60  # minute

# === 3. Aplică per (emp_name, session_name)
summary = df.groupby(['emp_name', 'session_name']).apply(total_active_minutes).reset_index()
summary.columns = ['emp_name', 'session_name', 'total_minutes']
summary['total_minutes'] = summary['total_minutes'].astype(int)

# === 4. Export CSV
summary.to_csv("attendance_summary_clean.csv", index=False)
print("✅ Fișier salvat: attendance_summary_clean.csv")
