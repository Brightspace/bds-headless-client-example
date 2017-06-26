 INSERT INTO users
    SELECT
        user_id,
        user_name,
        org_defined_id,
        first_name,
        middle_name,
        last_name,
        is_active,
        Organization,
        internal_email,
        external_email,
        signup_date
    FROM tmp_users
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET
    user_name = EXCLUDED.user_name,
    org_defined_id = EXCLUDED.org_defined_id,
    first_name = EXCLUDED.first_name,
    middle_name = EXCLUDED.middle_name,
    last_name = EXCLUDED.last_name,
    is_active = EXCLUDED.is_active,
    organization = EXCLUDED.organization,
    internal_email = EXCLUDED.internal_email,
    external_email = EXCLUDED.external_email,
    signup_date = EXCLUDED.signup_date
