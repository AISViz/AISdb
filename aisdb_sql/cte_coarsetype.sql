ref AS MATERIALIZED ( 
  SELECT 
    r.coarse_type, 
    r.coarse_type_txt
  FROM coarsetype_ref as r
)
