emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

grp_emp_dept = GROUP emp BY depno;

avg_sal_dept = FOREACH grp_emp_dept GENERATE 
    group AS depno,
    AVG(emp.salaire) AS salaire_moyen;

sorted_avg_dept = ORDER avg_sal_dept BY depno;

DUMP sorted_avg_dept;

STORE sorted_avg_dept INTO 'pigout/avg_salary_by_dept' USING PigStorage(',');
------------------------------------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

grp_emp_dept = GROUP emp BY depno;

cnt_emp_dept = FOREACH grp_emp_dept GENERATE 
    group AS depno,
    COUNT(emp) AS nb_employes;

sorted_cnt_dept = ORDER cnt_emp_dept BY depno;

DUMP sorted_cnt_dept;

STORE sorted_cnt_dept INTO 'pigout/count_by_dept' USING PigStorage(',');
--------------------------------------------------

emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

dept = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

join_emp_dept = JOIN emp BY depno, dept BY depno;

emp_dept_out = FOREACH join_emp_dept GENERATE 
    emp::id AS id,
    emp::nom AS nom,
    emp::prenom AS prenom,
    dept::name AS departement,
    emp::region AS region,
    emp::salaire AS salaire;

sorted_emp_dept = ORDER emp_dept_out BY id;

DUMP sorted_emp_dept;

STORE sorted_emp_dept INTO 'pigout/emp_with_dept' USING PigStorage(',');
------------------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

emp_high_sal = FILTER emp BY salaire > 60000;

emp_high_sal_sorted = ORDER emp_high_sal BY salaire DESC;

DUMP emp_high_sal_sorted;

STORE emp_high_sal_sorted INTO 'pigout/high_salary_emp' USING PigStorage(',');
----------------------------------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

emp_high_sal = FILTER emp BY salaire > 60000;

emp_high_sal_sorted = ORDER emp_high_sal BY salaire DESC;

DUMP emp_high_sal_sorted;

STORE emp_high_sal_sorted INTO 'pigout/high_salary_emp' USING PigStorage(',');
--------------------------------------------------------------------

emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

grp_emp_dept = GROUP emp BY depno;

max_sal_dept = FOREACH grp_emp_dept GENERATE 
    group AS depno,
    MAX(emp.salaire) AS max_salaire;

max_sal_dept_sorted = ORDER max_sal_dept BY max_salaire DESC;

top_sal_dept = LIMIT max_sal_dept_sorted 1;

DUMP top_sal_dept;

STORE top_sal_dept INTO 'pigout/top_salary_dept' USING PigStorage(',');
---------------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

dept = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

join_dept_emp = JOIN dept BY depno LEFT OUTER, emp BY depno;

dept_empty = FILTER join_dept_emp BY emp::id IS NULL;

dept_empty_out = FOREACH dept_empty GENERATE 
    dept::depno AS depno,
    dept::name AS name;

DUMP dept_empty_out;

STORE dept_empty_out INTO 'pigout/empty_depts' USING PigStorage(',');
-------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

grp_all_emp = GROUP emp ALL;

total_emp = FOREACH grp_all_emp GENERATE 
    COUNT(emp) AS total_employes;

DUMP total_emp;

STORE total_emp INTO 'pigout/total_employees' USING PigStorage(',');
-------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

dept = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

emp_paris = FILTER emp BY region == 'Paris';

emp_paris_dept = JOIN emp_paris BY depno, dept BY depno;

emp_paris_out = FOREACH emp_paris_dept GENERATE 
    emp_paris::id AS id,
    emp_paris::nom AS nom,
    emp_paris::prenom AS prenom,
    dept::name AS departement,
    emp_paris::salaire AS salaire;

sorted_emp_paris = ORDER emp_paris_out BY id;

DUMP sorted_emp_paris;

STORE sorted_emp_paris INTO 'pigout/paris_employees' USING PigStorage(',');
-------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

grp_emp_city = GROUP emp BY region;

sal_city = FOREACH grp_emp_city GENERATE 
    group AS ville,
    SUM(emp.salaire) AS salaire_total;

sal_city_sorted = ORDER sal_city BY salaire_total DESC;

DUMP sal_city_sorted;

STORE sal_city_sorted INTO 'pigout/total_salary_by_city' USING PigStorage(',');
-----------------------------------------------------------------------
emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

dept = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

emp_f = FILTER emp BY (
    prenom == 'Sophie' OR prenom == 'Marie' OR prenom == 'Claire' OR 
    prenom == 'Anne' OR prenom == 'Julie' OR prenom == 'Emma' OR 
    prenom == 'Chloé' OR prenom == 'Léa' OR prenom == 'Camille' OR 
    prenom == 'Manon'
);

emp_f_dept = JOIN emp_f BY depno, dept BY depno;

grp_f_dept = GROUP emp_f_dept BY dept::name;

out_f_dept = FOREACH grp_f_dept GENERATE 
    group AS departement,
    COUNT(emp_f_dept) AS nb_femmes;

sorted_out_f_dept = ORDER out_f_dept BY nb_femmes DESC;

DUMP sorted_out_f_dept;

STORE sorted_out_f_dept INTO 'pigout/employes_femmes' USING PigStorage(',');

