emp = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

dept = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

emp_sample = LIMIT emp 5;
DUMP emp_sample;

DESCRIBE emp;

grp_emp_dept = GROUP emp BY depno;

avg_sal_dept = FOREACH grp_emp_dept GENERATE 
    group AS depno,
    AVG(emp.salaire) AS salaire_moyen;

DUMP avg_sal_dept;

STORE avg_sal_dept INTO 'pigout/avg_salary_by_dept' USING PigStorage(',');

cnt_emp_dept = FOREACH grp_emp_dept GENERATE 
    group AS depno,
    COUNT(emp) AS nb_employes;

DUMP cnt_emp_dept;
STORE cnt_emp_dept INTO 'pigout/count_by_dept' USING PigStorage(',');

join_emp_dept = JOIN emp BY depno, dept BY depno;

emp_dept_out = FOREACH join_emp_dept GENERATE 
    emp::id AS id,
    emp::nom AS nom,
    emp::prenom AS prenom,
    dept::name AS departement,
    emp::region AS region,
    emp::salaire AS salaire;

DUMP emp_dept_out;
STORE emp_dept_out INTO 'pigout/emp_with_dept' USING PigStorage(',');

emp_high_sal = FILTER emp BY salaire > 60000;

emp_high_sal_sorted = ORDER emp_high_sal BY salaire DESC;

DUMP emp_high_sal_sorted;
STORE emp_high_sal_sorted INTO 'pigout/high_salary_emp' USING PigStorage(',');

max_sal_dept = FOREACH grp_emp_dept GENERATE 
    group AS depno,
    MAX(emp.salaire) AS max_salaire;

max_sal_dept_sorted = ORDER max_sal_dept BY max_salaire DESC;

top_sal_dept = LIMIT max_sal_dept_sorted 1;

DUMP top_sal_dept;
STORE top_sal_dept INTO 'pigout/top_salary_dept' USING PigStorage(',');

join_dept_emp = JOIN dept BY depno LEFT OUTER, emp BY depno;

empty_dept = FILTER join_dept_emp BY emp::id IS NULL;

empty_dept_out = FOREACH empty_dept GENERATE 
    dept::depno AS depno,
    dept::name AS name;

DUMP empty_dept_out;
STORE empty_dept_out INTO 'pigout/empty_depts' USING PigStorage(',');

grp_all_emp = GROUP emp ALL;

total_emp = FOREACH grp_all_emp GENERATE 
    COUNT(emp) AS total_employes;

DUMP total_emp;
STORE total_emp INTO 'pigout/total_employees' USING PigStorage(',');

emp_paris = FILTER emp BY region == 'Paris';

emp_paris_dept = JOIN emp_paris BY depno, dept BY depno;

emp_paris_out = FOREACH emp_paris_dept GENERATE 
    emp_paris::id AS id,
    emp_paris::nom AS nom,
    emp_paris::prenom AS prenom,
    dept::name AS departement,
    emp_paris::salaire AS salaire;

DUMP emp_paris_out;
STORE emp_paris_out INTO 'pigout/paris_employees' USING PigStorage(',');

grp_city = GROUP emp BY region;

sal_city = FOREACH grp_city GENERATE 
    group AS ville,
    SUM(emp.salaire) AS salaire_total;

sal_city_sorted = ORDER sal_city BY salaire_total DESC;

DUMP sal_city_sorted;
STORE sal_city_sorted INTO 'pigout/total_salary_by_city' USING PigStorage(',');

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

DUMP out_f_dept;

STORE out_f_dept INTO 'pigout/employes_femmes' USING PigStorage(',');

