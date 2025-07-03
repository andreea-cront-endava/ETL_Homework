import pandas as pd
from datetime import datetime
import oracledb
import os
import re

# === 1. Setări generale ===
folder = r"C:\Endava\EndevLocal\Tema_etl\.venv\Scripts\attendence excels"
file_pattern = r"training_attendance_(\d{4})_(\d{2})_(\d{2})\.csv"

# === 2. Conectare Oracle ===
conn = oracledb.connect(user="sources", password="sources123", dsn="localhost:1521/XEPDB1")
cursor = conn.cursor()

# === 3. Funcție de conversie durată → minute ===
def duration_to_minutes(duration_str):
    try:
        parts = duration_str.strip().split(' ')
        total_minutes = 0
        for part in parts:
            if 'h' in part:
                total_minutes += int(part.replace('h', '')) * 60
            elif 'm' in part:
                total_minutes += int(part.replace('m', ''))
        return total_minutes
    except:
        return 0

# === 4. Parcurgere fișiere ===
for filename in os.listdir(folder):
    match = re.match(file_pattern, filename)
    if not match:
        continue

    year, month, day = match.groups()
    dataset_label = f"TRAINING_{year}_{month}_{day}"
    session_name = f"ETL Theory training - {year}-{month}-{day}"
    file_path = os.path.join(folder, filename)

    print(f"📥 Import: {filename} → {dataset_label}")

    # === 5. Citește CSV ===
    df = pd.read_csv(file_path, skiprows=9)
    print(f"📂 Fișier: {filename} | Rânduri încărcate: {len(df)}")
    print("🧪 Coloane detectate:", df.columns.tolist())

    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        'Name': 'emp_name',
        'First Join': 'first_join',
        'Last Leave': 'last_leave',
        'In-Meeting Duration': 'in_meeting',
        'Email': 'emp_email',
        'Role': 'role'
    })

    # Elimină rânduri goale și rânduri cu "Join Time" în loc de dată
    df = df[df['emp_name'].notna() & df['first_join'].notna()]
    df = df[~df['first_join'].astype(str).str.contains("Join Time", na=False)]

    # Conversie date + oră
    df['start_time'] = pd.to_datetime(df['first_join'], format="%m/%d/%y, %I:%M:%S %p", errors='coerce')
    df['end_time'] = pd.to_datetime(df['last_leave'], format="%m/%d/%y, %I:%M:%S %p", errors='coerce')
    df = df[df['start_time'].notna() & df['end_time'].notna()]

    # Conversie durată în minute
    df['duration_min'] = df['in_meeting'].apply(duration_to_minutes)
    df['duration_min'] = pd.to_numeric(df['duration_min'], errors='coerce').fillna(0).astype(int)

    # Curățare câmpuri care pot da erori la inserare
    df['role'] = df['role'].fillna('')
    df['emp_email'] = df['emp_email'].fillna('')

    # Adaugă coloane Oracle
    df['session_name'] = session_name
    df['load_timestamp'] = datetime.now()
    df['dataset_label'] = dataset_label

    # === 6. Inserare în Oracle ===
    for idx, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO attendance (
                    emp_name, emp_email, session_name,
                    start_time, end_time, duration_min, role,
                    load_timestamp, dataset_label
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, :9
                )
            """, (
                row['emp_name'],
                row['emp_email'],
                row['session_name'],
                row['start_time'],
                row['end_time'],
                int(row['duration_min']),
                row['role'],
                row['load_timestamp'],
                row['dataset_label']
            ))
        except Exception as e:
            print(f"❌ Eroare la rândul {idx}: {row.to_dict()}")
            print(f"⛔ Eroare Oracle: {e}")

conn.commit()
cursor.close()
conn.close()

print("✅ Import complet pentru toate fișierele din folder.")
