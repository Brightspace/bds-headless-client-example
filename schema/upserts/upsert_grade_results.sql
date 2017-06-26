INSERT INTO grade_results
    SELECT
        grade_object_id,
        org_unit_id,
        user_id,
        points_numerator,
        points_denominator,
        weighted_numerator,
        weighted_denominator,
        is_released,
        is_dropped,
        last_modified,
        last_modified_by,
        comments,
        private_comments
    FROM tmp_grade_results
ON CONFLICT ON CONSTRAINT grade_results_pkey
DO UPDATE SET
    points_numerator = EXCLUDED.points_numerator,
    points_denominator = EXCLUDED.points_denominator,
    weighted_numerator = EXCLUDED.weighted_numerator,
    weighted_denominator = EXCLUDED.weighted_denominator,
    is_released = EXCLUDED.is_released,
    is_dropped = EXCLUDED.is_dropped,
    last_modified = EXCLUDED.last_modified,
    last_modified_by = EXCLUDED.last_modified_by,
    comments = EXCLUDED.comments,
    private_comments = EXCLUDED.private_comments
