INSERT INTO org_units
    SELECT
        org_unit_id,
        organization,
        type,
        name,
        code,
        start_date,
        end_date,
        is_active,
        created_date
    FROM tmp_org_units
ON CONFLICT ON CONSTRAINT org_units_pkey
DO UPDATE SET
    organization = EXCLUDED.organization,
    type = EXCLUDED.type,
    name = EXCLUDED.name,
    code = EXCLUDED.code,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    is_active = EXCLUDED.is_active,
    created_date = EXCLUDED.created_date
