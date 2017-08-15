CREATE TABLE IF NOT EXISTS grade_results
(
    grade_object_id BIGINT,
    org_unit_id BIGINT,
    user_id BIGINT,
    points_numerator DECIMAL,
    points_denominator DECIMAL,
    weighted_numerator DECIMAL,
    weighted_denominator DECIMAL,
    is_released BOOLEAN,
    is_dropped BOOLEAN,
    last_modified TIMESTAMP,
    last_modified_by BIGINT,
    comments TEXT,
    private_comments TEXT,
    is_exempt BOOLEAN,
    PRIMARY KEY (grade_object_id, org_unit_id, user_id)
)
