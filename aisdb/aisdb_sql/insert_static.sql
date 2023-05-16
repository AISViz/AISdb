INSERT INTO ais_{}_static (
    mmsi,
    time,
    vessel_name,
    ship_type,
    call_sign,
    imo,
    dim_bow,
    dim_stern,
    dim_port,
    dim_star,
    draught,
    destination,
    ais_version,
    fixing_device,
    eta_month,
    eta_day,
    eta_hour,
    eta_minute,
    source
  )
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT DO NOTHING
;
