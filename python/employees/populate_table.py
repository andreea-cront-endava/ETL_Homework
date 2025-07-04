from pathlib import Path
import pandas as pd
import oracledb

script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent

#conectarea la flat table book1.csv din folderul database
csv_path = project_root / "database" / "Book1.csv"

#citirea datelor din CSV
df = pd.read_csv(csv_path, usecols=[
    'first_name', 'last_name', 'email', 'is_active', 'hired_date', 'manager_id', 'location_id'
])

#conectarea la baza de date Oracle cu userul davax_user
dsn = oracledb.makedsn("localhost", 1521, service_name="XEPDB1")
conn = oracledb.connect(user="davax_user", password="Timesheet2025", dsn=dsn)
cursor = conn.cursor()

#pregatirea  pentru inserarea datelor
sql = """
INSERT /*+ APPEND */ INTO employees (
  first_name, last_name, email, is_active,
  hired_date, manager_id, location_id
) VALUES (
  :1, :2, :3, :4, TO_DATE(:5,'YYYY-MM-DD'), :6, :7
)
"""

#inserarea datelor in tabela EMPLOYEES
data = [tuple(r) for r in df.itertuples(index=False, name=None)]
cursor.executemany(sql, data)
conn.commit()

#mesaj de confirmare a inserarii 
print(f"Inserted {cursor.rowcount} rows into EMPLOYEES")

cursor.close()
conn.close()


