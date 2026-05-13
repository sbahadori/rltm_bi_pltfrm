SELECT {columns}
FROM {source_table}
WHERE {incremental_column} > {lower_bound}
  AND {incremental_column} <= {upper_bound}