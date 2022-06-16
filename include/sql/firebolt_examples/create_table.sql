CREATE FACT TABLE IF NOT EXISTS {{ conn.firebolt_default.schema }}.{{ params.table }}
(
    id INT,
    y INT,
    month VARCHAR(25),
    day VARCHAR(25),
    ffmc FLOAT,
    dmc FLOAT,
    dc FLOAT,
    isi FLOAT,
    temp FLOAT,
    rh FLOAT,
    wind FLOAT,
    rain FLOAT,
    area FLOAT
) PRIMARY INDEX id;
