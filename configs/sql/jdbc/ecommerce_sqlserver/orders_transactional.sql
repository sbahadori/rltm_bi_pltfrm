SELECT {columns}
FROM {source_table}
WHERE {transaction_column} > {lower_bound}
  AND {transaction_column} <= {upper_bound}