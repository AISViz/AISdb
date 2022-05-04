INSERT OR IGNORE INTO ais_{}_static (
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
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
