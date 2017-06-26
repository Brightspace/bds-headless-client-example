CREATE TABLE IF NOT EXISTS org_units
(
    org_unit_id BIGINT PRIMARY KEY,
    organization TEXT,
    type TEXT,
    name TEXT,
    code TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_active BOOLEAN,
    created_date TIMESTAMP
)
