CREATE VIRTUAL TABLE IF NOT EXISTS gebco_2022 USING rtree(
  id,
  x0, x1,
  y0, y1,
  +depth_metres INT
);
