CREATE TABLE IF NOT EXISTS users
(
    UserId BIGINT PRIMARY KEY,
    UserName TEXT,
    OrgDefinedId TEXT,
    FirstName TEXT,
    MiddleName TEXT,
    LastName TEXT,
    IsActive BOOLEAN,
    Organization TEXT,
    InternalEmail TEXT,
    ExternalEmail TEXT,
    SignupDate TIMESTAMP
)
