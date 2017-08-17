INSERT INTO user_enrollments
    SELECT
        org_unit_id,
        user_id,
        role_name,
        enrollment_date,
        enrollment_type
    FROM tmp_user_enrollments
ON CONFLICT ON CONSTRAINT user_enrollments_pkey
DO UPDATE SET
    role_name = EXCLUDED.role_name,
    enrollment_date = EXCLUDED.enrollment_date,
    enrollment_type = EXCLUDED.enrollment_type
