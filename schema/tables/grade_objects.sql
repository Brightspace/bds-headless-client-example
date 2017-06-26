CREATE TABLE IF NOT EXISTS grade_objects
(
    grade_object_id BIGINT PRIMARY KEY,
    org_unit_id BIGINT,
    parent_grade_object_id BIGINT,
    name TEXT,
    type_name TEXT,
    category_name TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_auto_pointed BOOLEAN,
    is_formula BOOLEAN,
    is_bonus BOOLEAN,
    max_points DECIMAL,
    can_exceed_max_grade BOOLEAN,
    exclude_from_final_grade_calc BOOLEAN,
    grade_scheme_id BIGINT,
    weight DECIMAL,
    num_lowest_grades_to_drop INTEGER,
    num_highest_grades_to_drop INTEGER,
    weight_distribution_type TEXT,
    created_date TIMESTAMP,
    tool_name TEXT,
    associated_tool_item_id BIGINT,
    last_modified TIMESTAMP
)