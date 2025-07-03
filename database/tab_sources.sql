SELECT USER FROM dual;

-- creare tabel TIMESHEET
CREATE TABLE timesheet (
    emp_id         VARCHAR2(10),
    work_date      DATE,
    project_code   VARCHAR2(20),
    hours_worked   NUMBER(4,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataset_label  VARCHAR2(20)
);

ALTER TABLE timesheet ADD emp_name VARCHAR2(100);

ALTER TABLE timesheet ADD status VARCHAR2(10);


SELECT * FROM TIMESHEET;


-- cate ore au lucrat toata saptamana fiecare
SELECT
    emp_id,
    emp_name,
    TO_CHAR(work_date, 'YYYY-MM-DD') AS zi,
    status,
    hours_worked
FROM
    sources.timesheet
WHERE
    dataset_label = 'WEEK_29'
ORDER BY
    emp_name, work_date;


DELETE FROM sources.timesheet WHERE dataset_label = 'WEEK_29';
COMMIT;


CREATE TABLE attendance (
    emp_name       VARCHAR2(100),
    emp_email      VARCHAR2(100),
    session_name   VARCHAR2(100),
    start_time     TIMESTAMP,
    end_time       TIMESTAMP,
    duration_min   NUMBER(5),
    role           VARCHAR2(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataset_label  VARCHAR2(20)
);

--creare tabel absences(concedii, medical)
CREATE TABLE absences (
    emp_id         VARCHAR2(20),
    emp_name       VARCHAR2(100),
    absence_type   VARCHAR2(50),  -- ex: 'Annual Leave', 'Sick Leave', 'Public Holiday'
    start_date     DATE,
    end_date       DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataset_label  VARCHAR2(20)
);


--creare tabel exam_absence(pentru examene, licenta, disertatie etc)
CREATE TABLE exam_absence (
    emp_id         VARCHAR2(20),
    emp_name       VARCHAR2(100),
    exam_date      DATE,
    absence_reason VARCHAR2(100),  -- ex: 'Bachelor’s Exam', 'Thesis Defense'
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataset_label  VARCHAR2(20)
);



SELECT emp_name, SUM(duration_min) AS total_minute
FROM attendance
GROUP BY emp_name
ORDER BY total_minute DESC;


-- per sesiune
SELECT session_name, SUM(duration_min) AS total_minutes
FROM attendance
GROUP BY session_name
ORDER BY session_name;

SELECT dataset_label, COUNT(DISTINCT emp_name) AS nr_participanti_unici
FROM attendance
GROUP BY dataset_label;

SELECT dataset_label, COUNT(DISTINCT emp_email) AS nr_participanti_unici
FROM attendance
GROUP BY dataset_label;

SELECT emp_name, dataset_label, SUM(duration_min) AS total_min
FROM attendance
WHERE emp_name = 'Andreea Cront'
GROUP BY emp_name, dataset_label;



SELECT
    emp_name,
    emp_email,
    session_name,
    ROUND(SUM((CAST(end_time AS DATE) - CAST(start_time AS DATE)) * 24 * 60)) AS total_minutes
FROM attendance
GROUP BY emp_name, emp_email, session_name
ORDER BY session_name, emp_name;



CREATE OR REPLACE VIEW attendance_summary_by_time AS
SELECT
    emp_name,
    emp_email,
    session_name,
    ROUND(SUM((CAST(end_time AS DATE) - CAST(start_time AS DATE)) * 24 * 60)) AS total_minutes
FROM attendance
GROUP BY emp_name, emp_email, session_name;


SELECT * FROM attendance_summary_by_time
ORDER BY total_minutes DESC;



CREATE TABLE attendance_clean (
    emp_name       VARCHAR2(100),
    emp_email      VARCHAR2(100),
    session_name   VARCHAR2(100),
    total_minutes  NUMBER(5)
);


SELECT COUNT(*) FROM attendance_clean;

SELECT * FROM attendance_clean
FETCH FIRST 10 ROWS ONLY;

SELECT *
FROM attendance_clean
WHERE session_name = 'ETL Theory training - 2025-06-24';



SELECT emp_name, exam_date, absence_reason, dataset_label
FROM exam_absence
ORDER BY load_timestamp DESC
FETCH FIRST 20 ROWS ONLY;
SELECT emp_name, exam_date, absence_reason, dataset_label, load_timestamp
FROM exam_absence
ORDER BY load_timestamp DESC
FETCH FIRST 20 ROWS ONLY;

SELECT emp_name, absence_type, start_date, end_date, dataset_label, load_timestamp
FROM absences
ORDER BY load_timestamp DESC
FETCH FIRST 20 ROWS ONLY; 


SHOW CON_NAME;
