import pandas as pd
from datetime import datetime
import oracledb

# 1. Citește fișierul CSV
df = pd.read_csv("timesheet_full_iasi.csv")

# 2. Adaugă coloane tehnice
df['load_timestamp'] = datetime.now()
df['dataset_label'] = 'WEEK_29'

# 3. Normalizează orele lucrate astfel încât fiecare să aibă exact 40/săptămână
adjusted_rows = []
for emp_id, group in df.groupby('emp_id'):
    work_days = group[group['status'] == 'work']
    absence_days = group[group['status'] == 'absence']
    nr_workdays = len(work_days)

    if nr_workdays == 0:
        continue

    per_day_hours = 40 / nr_workdays  # ex: 10 ore dacă doar 4 zile muncite

    for idx, row in group.iterrows():
        new_row = row.copy()
        new_row['hours_worked'] = per_day_hours if row['status'] == 'work' else 0
        adjusted_rows.append(new_row)

df_adjusted = pd.DataFrame(adjusted_rows)

# 4. Conectare la Oracle
conn = oracledb.connect(user="sources", password="sources123", dsn="localhost:1521/XEPDB1")
cursor = conn.cursor()

# 5. Insert rând cu rând în tabel
for _, row in df_adjusted.iterrows():
    cursor.execute("""
        INSERT INTO timesheet (
            emp_id, emp_name, work_date, project_code, status,
            hours_worked, load_timestamp, dataset_label
        )
        VALUES (
            :1, :2, TO_DATE(:3, 'YYYY-MM-DD'), :4, :5,
            :6, :7, :8
        )
    """, (
        row['emp_id'],
        row['emp_name'],
        row['work_date'],
        row['project_code'],
        row['status'],
        row['hours_worked'],
        row['load_timestamp'],
        row['dataset_label']
    ))

# 6. Commit & închide conexiunea
conn.commit()
cursor.close()
conn.close()

print("✅ Datele au fost importate în Oracle în tabela SOURCES.TIMESHEET.")
