-- 1. Scalar Function (Returns Single Value)
-- Returns one value (INT, BIGINT, VARCHAR, DATE etc.)

CREATE FUNCTION dbo.fn_GetUserStatus
(
    @IsActive BIT
)
RETURNS VARCHAR(20)
AS
BEGIN
    DECLARE @Status VARCHAR(20);

    SET @Status = CASE 
                    WHEN @IsActive = 1 THEN 'Active'
                    ELSE 'Inactive'
                  END;

    RETURN @Status;
END;

-- Calling
SELECT dbo.fn_GetUserStatus(1) AS UserStatus;




-- 2. Inline Table-Valued Function (Returns Table) Best for Performance
-- Returns a table using only one SELECT (no BEGIN/END)

CREATE FUNCTION dbo.fn_GetUserDetails
(
    @Userid BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT LoginId, UserEmail, CreatedDate, ModifiedDate, IsActive, SetupId
    FROM dbo.UserLoginDetail
    WHERE UserId = @Userid
);

-- Calling
SELECT * FROM dbo.fn_GetUserDetails(101);




-- 3) Multi-Statement Table-Valued Function (Returns Table)
-- Used when you need multiple steps before returning result table.

CREATE FUNCTION dbo.fn_GetActiveUsers()
RETURNS @Result TABLE
(
    LoginId      BIGINT,
    UserEmail    VARCHAR(100),
    CreatedDate  DATETIME,
    ModifiedDate DATETIME,
    SetupId      BIGINT
)
AS
BEGIN
    INSERT INTO @Result
    SELECT LoginId, UserEmail, CreatedDate, ModifiedDate, SetupId
    FROM dbo.UserLoginDetail
    WHERE IsActive = 1;

    RETURN;
END;

-- Calling
SELECT * FROM dbo.fn_GetActiveUsers();




-- 4) Example: Function with Default Parameter
CREATE FUNCTION dbo.fn_GetUsersByStatus
(
    @IsActive BIT = 1
)
RETURNS TABLE
AS
RETURN
(
    SELECT *
    FROM dbo.UserLoginDetail
    WHERE IsActive = @IsActive
);

-- Calling
SELECT * FROM dbo.fn_GetUsersByStatus();      -- default IsActive = 1
SELECT * FROM dbo.fn_GetUsersByStatus(0);     -- inactive users


-- Drop Function Syntax
DROP FUNCTION dbo.fn_GetUserStatus;




