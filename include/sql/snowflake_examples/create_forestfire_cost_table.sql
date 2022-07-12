CREATE OR REPLACE TRANSIENT TABLE {{ params.table_name }}
  (id INT,
    land_damage_cost INT,
    property_damage_cost INT,
    lost_profits_cost INT,
    total_cost INT,
    y INT,
    month VARCHAR(25),
    day VARCHAR(25),
    area FLOAT);
