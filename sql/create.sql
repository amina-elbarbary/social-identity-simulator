CREATE TABLE IF NOT EXISTS indicator (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    option TEXT NOT NULL,
    CONSTRAINT pk_indicator
        PRIMARY KEY (id),
    CONSTRAINT uq_indicator
        UNIQUE (name, option)
);

CREATE TABLE IF NOT EXISTS combination (
    id INTEGER NOT NULL,
    CONSTRAINT pk_combination
        PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS combination_indicator (
    combination_id INTEGER NOT NULL,
    indicator_id INTEGER NOT NULL,
    CONSTRAINT pk_combination_indicator
        PRIMARY KEY (combination_id, indicator_id),
    CONSTRAINT fk_combination_indicator_combination
        FOREIGN KEY (combination_id)
            REFERENCES combination(id),
    CONSTRAINT fk_combination_indicator_indicator
        FOREIGN KEY (indicator_id)
            REFERENCES indicator(id)
);

CREATE TABLE IF NOT EXISTS distances (
    year integer NOT NULL,
    combination_id integer NOT NULL,
    indicator text NOT NULL,
    indicator_type TEXT,
    value FLOAT,
    CONSTRAINT pk_distances
        PRIMARY KEY (year, combination_id, indicator, indicator_type)
);
