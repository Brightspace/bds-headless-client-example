CREATE TABLE IF NOT EXISTS users
(
    user_id BIGINT PRIMARY KEY,
    user_name TEXT,
    org_defined_id TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    is_active BOOLEAN,
    organization TEXT,
    internal_email TEXT,
    external_email TEXT,
    signup_date TIMESTAMP
)
