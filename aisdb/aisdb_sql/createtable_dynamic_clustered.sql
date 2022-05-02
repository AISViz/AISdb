CREATE TABLE IF NOT EXISTS ais_{}_dynamic (
            mmsi INTEGER NOT NULL,
            time INTEGER NOT NULL,
            longitude FLOAT NOT NULL,
            latitude FLOAT NOT NULL,
            rot FLOAT,
            sog FLOAT,
            cog FLOAT,
            heading FLOAT,
            maneuver TEXT,
            utc_second INTEGER,
            PRIMARY KEY (mmsi, time, longitude, latitude)
        ) WITHOUT ROWID;
