"""
Parses schema definitions in include/libs/table_schemas/snowflake/<db_name>/<schema_name>.json
"""
import json
import logging
import pandas as pd
import numpy as np

### RELIES ON TEMPORARY SCHEMA DESIGN FOR ORDERING #####
# "id": {
#     "type": "varchar(25)",
#     "description": "source_order=1"
# }
def get_schema_source_col(key, prop):
    """
    Temporary solution to map source columns to
    destination columns.
    Added due to some unexpected column renaming in houston
    and needing fixing immediately.
    """
    if prop.get('default_source', None):
        return prop.get('default_source').split('=')[1]
    else:
        return key

def get_schema_order(prop: dict,) -> str:
    """
    Get 'description' field from schema definition file
    """

    if prop.get('description', None):
        return int(prop.get('description').split('=')[1])
    else:
        return -1

def get_schema_type(prop: dict,) -> str:
    """
    Get 'type' field from schema definition file
    """

    return prop.get('type', None)


def get_schema_raw_columns(table_props: dict) -> list:
    """
    This can be used to add headers to a CSV or resolve
    columns that should only be included source data cleaning
    """
    return [key for key, val in table_props.items()
                if val.get('description') is not None]


def get_table_def_schema(TABLE_SCHEMA_DIR, transform_db, transform_schema):
    """
    Fow now, we'll access local json files. This is meant to evolve
    """
    with open(f'{TABLE_SCHEMA_DIR}/{transform_db}/{transform_schema}.json', 'r') as f:
        table_schema = json.load(f)
        table_def = table_schema.get('definitions')
        return table_def


def snowflake_load_column_string(table_props: dict) -> str:
    """
    Use the json table definition to build string necessary for
    selecting fields of interest for loading (i.e. omit passwords)
    Scrub it
    :param table_props: python dictionary of table properties dictionary
        from houston.json schema def
    :type table_props: dict
    :return col_string: string encoded for select on COPY ($1,$3,$6,et...)
    """
    # This should work with if discription is not None
    try:
        vals = [f"${val.get('description','').split('=')[1]}"
                for key, val in table_props.items()
                    if key not in ['insert_timestamp','hash_diff']
        ]
    except Exception as e:
        logging.error('Bad Table Def Schema %s' % e)
        raise
    col_string = ','.join(vals)
    return col_string


def resolve_schemas(df:pd.DataFrame, table_props: dict) -> pd.DataFrame:
    """
    Take dataframe with raw data and remove or rename columns to match
    table schema, and if any remain from schema that aren't in dataframe,
    set null types for those colums
    """
    df.columns = map(str.lower, df.columns)
    # get returned columns, the nmap returned column names to
    # new names old: new
    col_mapping = {get_schema_source_col(k, v): k.lower()
                        for k,v in table_props.items()}

    df.rename(columns=col_mapping, inplace=True)
    current_cols = df.columns.tolist()

    schema_orders = {k.lower(): get_schema_order(v)
                        for k,v in table_props.items()
                            if get_schema_order(v) != -1}

    schema_cols = list(schema_orders.keys())

    # Take the values we have defined in the schema and set the order
    schema_inters_cols = list(set(current_cols).intersection(schema_cols))
    schema_inters_cols.sort(key=schema_orders.__getitem__)

    if schema_inters_cols:
        df = df.loc[:,schema_inters_cols]
    else:
        raise ValueError(f"Bad Schema Design in Sorting Columns {df.columns.tolist()}")

    # Check if any columns exist in full schema cols
    # that aren't in schema intersection cols
    # If so, add them with null values
    remaining = list(set(schema_cols) - set(schema_inters_cols))
    if remaining:
        for col in remaining:
            prop = table_props.get(col)
            if prop.get('default', None):
                df.loc[:,col] =  prop.get('default')
            else:
                dtype = get_schema_type(prop).lower()
                if (('varchar' in dtype) or ('text' in dtype) or
                    ('string' in dtype)):
                    df.loc[:,col] = None
                elif (('number' in dtype) or ('timestamp' in dtype) or
                      ('float' in dtype) or ('int' in dtype)):
                    df.loc[:,col] = np.nan
                elif 'bool' in dtype:
                    df.loc[:,col] = False
                else:
                    df.loc[:,col] = ''

    #make sure we rearrange
    schema_cols.sort(key=schema_orders.__getitem__)
    return df.loc[:,schema_cols]
