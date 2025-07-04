--crearea utiliztorului care sa testeze sistemul
CREATE USER davax_user IDENTIFIED BY Timesheet2025 
    DEFAULT TABLESPACE users
    TEMPORARY TABLESPACE temp
    QUOTA UNLIMITED ON users;
GRANT CREATE SESSION TO davax_user;
GRANT RESOURCE TO davax_user;
GRANT DROP ANY TABLE TO davax_user;  