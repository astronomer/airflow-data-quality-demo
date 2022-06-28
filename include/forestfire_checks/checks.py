"""
Table Level Checks
"""
TABLE_CHECKS = [
    {"row_count_check": {"check_statement": "COUNT(*) = 9"}},
    {"dmc_less_than_twice_dc_check": {"check_statement": "2 * dmc < dc"}}
    # could be cool to check the table schema against known columns, as well
]

"""
Column Level Checks
"""
COL_CHECKS = [
    {"id": {
        "null_check": {"equal_to": 0},
        "distinct_check": {"equal_to": 9}
    }},
    {"ffmc": {
        "min": {"geq_to": 50},
        "max": {"less_than": 100}
    }},
]
