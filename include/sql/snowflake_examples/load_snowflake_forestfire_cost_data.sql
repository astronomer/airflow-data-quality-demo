INSERT INTO forestfire_cost
    SELECT (
        c.id,
        c.land_damage_cost,
        c.property_damage_cost,
        c.lost_profits_cost,
        c.land_damage_cost + c.property_damage_cost + c.lost_profits_cost AS total_cost,
        ff.y,
        ff.month,
        ff.day,
        ff.area,
    FROM costs
    LEFT JOIN forestfires ff
        ON c.id = ff.id
    )
