import pandas as pd
from datetime import datetime
import oracledb

# 1. Citește fișierul CSV
df = pd.read_csv("bhd.csv")

# 2. Normalizează coloane
df.columns = df.columns.str.strip()
df = df.rename(columns={
    'Meeting Organizer': 'emp_name',
    'Subject': 'absence_reason',
    'Start Date': 'start_date',
    'Start Time': 'start_time',
    'End Date': 'end_date'  # ← dacă ai și "End Date" în CSV
})

# 3. Creează coloane suplimentare
df['load_timestamp'] = datetime.now()
df['dataset_label'] = 'EXAM_JUNE_2025'

# 4. Normalizează formatul datelor
df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')  # optional

# 5. Filtrare

## exam-related
exam_keywords = [
    'exam', 'university test', 'faculty exam',
    'project presentation', 'test', 'graduation',
    'ridicare documente', 'ridicare', 'dissertation'
]

df_exam = df[df['absence_reason'].str.lower().apply(
    lambda x: any(kw in x for kw in exam_keywords)
)].copy()

df_exam['exam_date'] = df_exam['start_date'].dt.date

## absences (concedii, sărbători etc.)
df['absence_reason'] = df['absence_reason'].str.strip().str.lower()
absence_types = ['annual leave', 'sick leave', 'public holiday']
absence_keywords = ['annual leave', 'sick leave', 'public holiday']
df_absences = df[df['absence_reason'].apply(lambda x: any(kw in x for kw in absence_keywords))].copy()


df_absences['absence_type'] = df_absences['absence_reason']  # redenumim logic

# 6. Conectare Oracle
conn = oracledb.connect(user="sources", password="sources123", dsn="localhost:1521/XEPDB1")
cursor = conn.cursor()

# 7. Insert exam data în `exam_absence`
for _, row in df_exam.iterrows():
    cursor.execute("""
        INSERT INTO exam_absence (
            emp_id, emp_name, exam_date, absence_reason,
            load_timestamp, dataset_label
        ) VALUES (
            NULL, :1, :2, :3, :4, :5
        )
    """, (
        row['emp_name'],
        row['exam_date'],
        row['absence_reason'],
        row['load_timestamp'],
        row['dataset_label']
    ))

# 8. Insert leave data în `absences`
for _, row in df_absences.iterrows():
    cursor.execute("""
        INSERT INTO absences (
            emp_id, emp_name, absence_type, start_date, end_date,
            load_timestamp, dataset_label
        ) VALUES (
            NULL, :1, :2, :3, :4, :5, :6
        )
    """, (
        row['emp_name'],
        row['absence_type'],
        row['start_date'],
        row['end_date'],
        row['load_timestamp'],
        row['dataset_label']
    ))

conn.commit()
cursor.close()
conn.close()

print("✅ Import complet: examene în exam_absence, concedii în absences.")
