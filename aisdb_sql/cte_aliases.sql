dynamic_{} AS ( {}
),
static_{} AS ( {}
),
ref AS MATERIALIZED ( 
    SELECT 
        coarse_type, 
        coarse_type_txt
      FROM coarsetype_ref
)
