CREATE TABLE IF NOT EXISTS org_units
(
    OrgUnitId BIGINT PRIMARY KEY,
    Organization TEXT,
    Type TEXT,
    Name TEXT,
    Code TEXT,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    IsActive BOOLEAN,
    CreatedDate TIMESTAMP
)
