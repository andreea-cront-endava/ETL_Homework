------------------------------------------------TARGET TABLES
-------CREARE TABEL LOCATION

CREATE TABLE location (
    location_id VARCHAR2(20) PRIMARY KEY,
    city VARCHAR2(100) NOT NULL);
    
-------CREARE TABEL MANAGER

CREATE TABLE manager (
    manager_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR2(100) NOT NULL,
    last_name VARCHAR2(100) NOT NULL,
    email VARCHAR2(100) NOT NULL,
    location_id VARCHAR2(3) REFERENCES location(location_id));

-------CREARE TABEL EMPLOYEES

CREATE TABLE employees(
    employee_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR2(100) NOT NULL,
    last_name VARCHAR2(100) NOT NULL,
    email VARCHAR2(100) NOT NULL,
    is_active CHAR(1) DEFAULT 'Y' CHECK (is_active IN ('Y', 'N')),
    hired_date DATE,
    manager_id NUMBER REFERENCES manager(manager_id),
    location_id VARCHAR2(3) REFERENCES location(location_id));
    
-------CREARE TABEL EMPLOYEES_ARCHIVE 

--EMPLOYEE_ARCHIVE ESTE UN TABEL CARE CONTINE DATE DESCRIPTIVE DESPRE PARCURSUL ANGAJATULUI IN COMPANIE

CREATE TABLE employees_archive(
    attribute_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_id NUMBER NOT NULL REFERENCES employees(employee_id),
    discipline VARCHAR2(100) NOT NULL,
    job_position VARCHAR2(100) NOT NULL,
    manager_id NUMBER REFERENCES manager(manager_id),
    date_from DATE NOT NULL,
    date_to DATE, 
    modification_date DATE DEFAULT SYSDATE);
    
 -------CREARE TABEL DAILY_ACTIVITY
 
 CREATE TABLE daily_activity(
    activity_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_id NUMBER NOT NULL REFERENCES employees(employee_id),
    activity_date DATE NOT NULL,
    activity_type VARCHAR2(50) NOT NULL,
    activity_description VARCHAR2(500) NOT NULL,
    work_hours NUMBER(4,2),
    overtime_hours NUMBER(4,2),
    training_attendance CHAR(1) DEFAULT 'N' CHECK (training_attendance IN ('Y', 'N')),
    training_duration NUMBER(6),
    absence_type VARCHAR2(50),
    
    CONSTRAINT chk_activity_type CHECK (activity_type IN ('Work', 'Training', 'Absence', 'Exam')),
    CONSTRAINT chk_absence_type CHECK (absence_type IN ('Annual leave', 'Special leave', 'Medical leave', 'Unpaid leave')),
    CONSTRAINT chk_work_logic CHECK (
        (activity_type = 'Work' AND work_hours IS NOT NULL) OR 
         activity_type != 'Work'
    ),
    CONSTRAINT chk_training CHECK (
        (activity_type = 'Training' AND 
         training_attendance IN ('Y', 'N') AND
         ((training_attendance = 'Y' AND training_duration > 0) OR
         (training_attendance = 'N' AND training_duration = 0))
        ) OR 
        activity_type != 'Training'
    ),
    CONSTRAINT chk_absence CHECK (
        (activity_type = 'Absence' AND 
         absence_type IS NOT NULL AND
         work_hours = 0 AND 
         overtime_hours = 0 AND
         training_attendance = 'N' AND
         training_duration = 0
        ) OR 
        activity_type != 'Absence'
    ));
    
 ---------------------------------POPULARE TABELE 
 
 ----------TAB LOCATION
INSERT INTO location (location_id, city) VALUES ('BHD', 'Bucuresti');
INSERT INTO location (location_id, city) VALUES ('ISD', 'Iasi');
INSERT INTO location (location_id, city) VALUES ('SV', 'Suceava');
INSERT INTO location (location_id, city) VALUES ('CLD', 'Cluj-Napoca');
INSERT INTO location (location_id, city) VALUES ('BVD', 'Brasov');
INSERT INTO location (location_id, city) VALUES ('TMD', 'Timisoara');
INSERT INTO location (location_id, city) VALUES ('PTD', 'Pitesti');
INSERT INTO location (location_id, city) VALUES ('TGD', 'Targu Mures');

 ----------TAB MANAGER
-- Manager 1: Bogdan Darabaneanu - Iasi
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Bogdan', 'Darabaneanu', 'Bogdan.Darabaneanu@endava.com', location_id
FROM location 
WHERE city = 'Iasi';

-- Manager 2: Mariana Apastinei - Suceava
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Mariana', 'Apastinei', 'Mariana.Apastinei@endava.com', location_id
FROM location 
WHERE city = 'Suceava';

-- Manager 3: Oana Macean - Timisoara
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Oana', 'Macean', 'Oana.Macean@endava.com', location_id
FROM location 
WHERE city = 'Timisoara';

-- Manager 4: Carmina Bernat - Targu Mures
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Carmina', 'Bernat', 'Carmina.Bernat@endava.com', location_id
FROM location 
WHERE city = 'Targu Mures';

-- Manager 5: Adrian Matean - Cluj-Napoca
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Adrian', 'Matean', 'Adrian.Matean@endava.com', location_id
FROM location 
WHERE city = 'Cluj-Napoca';

-- Manager 6: Andreea Cojocaru - Bucuresti
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Andreea', 'Cojocaru', 'Andreea.Cojocaru@endava.com', location_id
FROM location 
WHERE city = 'Bucuresti';

-- Manager 7: Ciprian Dogaru - Pitesti
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Ciprian', 'Dogaru', 'Ciprian.Dogaru@endava.com', location_id
FROM location 
WHERE city = 'Pitesti';

-- Manager 8: Petrisor Dima - Brasov
INSERT INTO manager (first_name, last_name, email, location_id) 
SELECT 'Petrisor', 'Dima', 'Petrisor.Dima@endava.com', location_id
FROM location 
WHERE city = 'Brasov';

 ----------TAB EMPLOYEES

--datele pentru tabelul employees au fost importate din fisierul Book1.csv prin
--intermediul scriptului populate_table.py localizat \python\employees\populate_table.py

UPDATE employees e
SET manager_id = (
    SELECT m.manager_id 
    FROM manager m 
    WHERE m.location_id = e.location_id
)
WHERE e.location_id IN (SELECT location_id FROM location);

 ----------TAB EMPLOYEES_ARCHIVE
 
INSERT INTO employees_archive(
    employee_id, 
    discipline, 
    job_position, 
    manager_id, 
    date_from, 
    date_to, 
    modification_date
) SELECT e.employee_id, 
    'Data' AS discipline,
    CASE --distribuire random a job position de "Junior Data Engineer" si "Data Engineer"
        WHEN MOD(e.employee_id, 2) = 0 THEN 'Junior Data Engineer'
        ELSE 'Data Engineer'
    END as job_position,
    e.manager_id,
    DATE '2025-05-26' as date_from,
    NULL as date_to,
    SYSDATE as modification_date
FROM employees e
WHERE e.is_active = 'Y';
