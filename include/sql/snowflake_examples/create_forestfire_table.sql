CREATE OR REPLACE TRANSIENT TABLE {{ params.table_name }}
  (id INT,
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
    area FLOAT);
