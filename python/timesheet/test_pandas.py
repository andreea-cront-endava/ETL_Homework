import pandas as pd
from datetime import datetime
import re

# === 1. Citește tabelele HTML ===
html_file = r"C:\Users\acront\OneDrive - ENDAVA\Desktop\Calendar - Time and Absences - Oracle Fusion Cloud Applications_Sapt1.html"
tables = pd.read_html(html_file)

# === 2. Extragere angajați ===
employees_df = tables[23]
employees_df.columns = ['emp_name', 'emp_id']
employees_df['emp_id'] = employees_df['emp_id'].astype(str).str.strip()
employees_df['emp_name'] = employees_df['emp_name'].astype(str).str.strip()

# === 3. Extragere date ===
dates_raw = tables[26].iloc[0, 0]
date_matches = re.findall(r'\d{1,2}/\d{1,2}/25', dates_raw)
dates = [datetime.strptime(d, "%m/%d/%y").strftime("%Y-%m-%d") for d in date_matches]

# === 4. Extragere ore (raw string "8 Ho", "9:00 AM–5:00 PM" etc.) ===
hours_raw = tables[28][0].tolist()
hours_matrix = []
for row in hours_raw:
    row_clean = row.replace("Ho", "").replace("–", "-")
    values = row_clean.split()
    row_numeric = []
    for val in values:
        try:
            h = int(val)
        except ValueError:
            h = 0
        row_numeric.append(h)
    hours_matrix.append(row_numeric)

# === 5. Construire tabel complet ===
# Identificăm doar zilele lucrătoare (Luni–Vineri)
workday_indices = []
workday_dates = []

for idx, date_str in enumerate(dates):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    if date_obj.weekday() < 5:
        workday_indices.append(idx)
        workday_dates.append(date_str)

# Creăm rândurile per angajat și zi lucrătoare
rows = []
for i, emp in employees_df.iterrows():
    emp_id = emp['emp_id']
    emp_name = emp['emp_name']
    total_workdays = 0  # pentru contor
    for j, idx in enumerate(workday_indices):
        date_str = workday_dates[j]
        hours = 0
        if i < len(hours_matrix):
            row_hours = hours_matrix[i]
            if j < len(row_hours):
                hours = row_hours[j]

        # Marcăm ziua ca "work" sau "absence"
        if hours >= 8:
            status = "work"
            total_workdays += 1
        else:
            status = "absence"

        rows.append({
            'emp_id': emp_id,
            'emp_name': emp_name,
            'work_date': date_str,
            'project_code': 'PRJ001',
            'status': status
        })

# === 6. Conversie în DataFrame și corectare ore (ca să totalizeze 40)
df_final = pd.DataFrame(rows)

# Se adaugă coloana 'hours_worked' = 8 doar pentru 'work', altfel 0
df_final['hours_worked'] = df_final['status'].apply(lambda x: 8 if x == 'work' else 0)

# Calculăm suma și normalizăm pentru ca fiecare angajat să aibă 40 ore
adjusted_rows = []
for emp_id, group in df_final.groupby('emp_id'):
    work_days = group[group['status'] == 'work']
    absence_days = group[group['status'] == 'absence']
    nr_workdays = len(work_days)

    if nr_workdays == 0:
        continue  # evităm împărțirea la 0

    # Recalculăm: fiecare zi lucrată primește 40 / nr_workdays
    per_day_hours = 40 // nr_workdays
    for idx, row in group.iterrows():
        new_row = row.copy()
        if row['status'] == 'work':
            new_row['hours_worked'] = per_day_hours
        else:
            new_row['hours_worked'] = 0
        adjusted_rows.append(new_row)

df_adjusted = pd.DataFrame(adjusted_rows)

# === 7. Salvare în CSV ===
df_adjusted.to_csv("timesheet_full_iasi.csv", index=False)
print(f"✅ Salvat: timesheet_full_iasi.csv cu {len(df_adjusted)} rânduri")
