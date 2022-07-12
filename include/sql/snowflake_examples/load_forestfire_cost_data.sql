INSERT INTO forestfire_costs (id, land_damage_cost, property_damage_cost, lost_profits_cost, total_cost, y, month, day, area)
    SELECT
        c.id,
        c.land_damage_cost,
        c.property_damage_cost,
        c.lost_profits_cost,
        c.land_damage_cost + c.property_damage_cost + c.lost_profits_cost,
        ff.y,
        ff.month,
        ff.day,
        ff.area
    FROM costs c
    LEFT JOIN forestfires ff
        ON c.id = ff.id
