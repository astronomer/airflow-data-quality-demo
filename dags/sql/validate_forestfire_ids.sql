SELECT ID FROM {{ params.redshift_table }} WHERE ID > 9 and ID < 1;
