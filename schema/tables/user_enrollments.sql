CREATE TABLE IF NOT EXISTS user_enrollments
(
    org_unit_id BIGINT,
    user_id BIGINT,
    role_name TEXT,
    enrollment_date TIMESTAMP,
    enrollment_type TEXT,
    PRIMARY KEY (org_unit_id, user_id)
)
