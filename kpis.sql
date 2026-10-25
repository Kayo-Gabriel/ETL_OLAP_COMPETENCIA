
/* 1 - Quantidade Total de Registros de RH na Fato*/
SELECT
    COUNT(*) AS total_registros_fato
FROM fhuman_resources;


/* 2 - Total de Horas de Licença Médica Concedidas*/
SELECT
    SUM(sickleavehours) AS total_geral_horas_licenca
FROM fhuman_resources;


/*3. Maior Salário por Hora Registrado*/
SELECT
    MAX(rate) AS maior_salario_hora
FROM fhuman_resources;


/* 4 - Menor Salário por Hora Registrado (Valor Mínimo)*/
SELECT
    MIN(rate) AS menor_salario_hora
FROM fhuman_resources;


/* 5 - Média Geral de Horas de Férias Acumuladas*/
SELECT
    AVG(vacationhours) AS media_geral_horas_ferias
FROM fhuman_resources;


/* 6 - Soma de Horas de Licença Médica por Departamento*/
SELECT
    d.name AS departamento,
    SUM(f.sickleavehours) AS total_horas_licenca
FROM fhuman_resources f
JOIN ddepartment d ON f.departmentid = d.departmentid
GROUP BY d.name;


/* 7 - Média Salarial por Gênero*/
SELECT
    e.gender AS genero,
    AVG(f.rate) AS media_salarial_hora
FROM fhuman_resources f
JOIN demployee e ON f.employeeid = e.employeeid
GROUP BY e.gender;


/* 8 - Total de Funcionários por Estado Civil*/
SELECT
    maritalstatus AS estado_civil,
    COUNT(*) AS total_colaboradores
FROM demployee
GROUP BY maritalstatus;


/* 9 - Total de Departamentos por Grupo Organizacional*/
SELECT
    groupname AS grupo_departamento,
    COUNT(*) AS total_departamentos
FROM ddepartment
GROUP BY groupname;


/* 10 - Média de Horas de Férias por Turno de Trabalho*/
SELECT
    s.name AS turno,
    AVG(f.vacationhours) AS media_horas_ferias
FROM fhuman_resources f
JOIN dshift s ON f.shiftid = s.shiftid
GROUP BY s.name;
