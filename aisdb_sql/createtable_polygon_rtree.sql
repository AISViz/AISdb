CREATE VIRTUAL TABLE IF NOT EXISTS rtree_polygons USING rtree(
    id,
    minX, maxX,
    minY, maxY,
    +objname TEXT,
    +domain TEXT,
    +binary BLOB
);
