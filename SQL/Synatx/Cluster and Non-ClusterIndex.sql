--1. Clustered Index Syntax
--Clustered index physically sorts/stores table data based on index key.

CREATE CLUSTERED INDEX IX_UserLoginDetail_UserId
ON dbo.UserLoginDetail (UserId);

DROP INDEX IX_UserLoginDetail_UserId ON dbo.UserLoginDetail;

--2) Non-Clustered Index Syntax
--Non-clustered index creates a separate structure pointing to the table rows.

CREATE NONCLUSTERED INDEX IX_UserLoginDetail_UserEmail
ON dbo.UserLoginDetail (UserEmail);

DROP INDEX IX_UserLoginDetail_UserEmail ON dbo.UserLoginDetail;

--3) Composite Index Syntax (Multi-column Index)
--Composite index = index on more than one column.


CREATE NONCLUSTERED INDEX IX_UserLoginDetail_UserId_IsActive
ON dbo.UserLoginDetail (UserId, IsActive);
--Note: Column order matters (UserId first then IsActive)


--4) Covering Index (INCLUDE) Syntax
--Covering index helps query get all required columns without lookup.


CREATE NONCLUSTERED INDEX IX_UserLoginDetail_UserId_Cover
ON dbo.UserLoginDetail (UserId)
INCLUDE (LoginId, UserEmail, CreatedDate, ModifiedDate, IsActive, SetupId);


--Best use case:

SELECT LoginId, UserEmail, CreatedDate, ModifiedDate, IsActive, SetupId
FROM dbo.UserLoginDetail
WHERE UserId = 101;


--5) Filtered Index Syntax
--Filtered index is created only on rows matching a condition (smaller + faster).

CREATE NONCLUSTERED INDEX IX_UserLoginDetail_ActiveUsers
ON dbo.UserLoginDetail (UserId)
WHERE IsActive = 1;

--Another Example (Non-null emails only)
CREATE NONCLUSTERED INDEX IX_UserLoginDetail_UserEmail_NotNull
ON dbo.UserLoginDetail (UserEmail)
WHERE UserEmail IS NOT NULL;

--Quick Combined Example (Advanced)

--Composite + INCLUDE + Filter together:

CREATE NONCLUSTERED INDEX IX_UserLoginDetail_UserId_IsActive_FilterCover
ON dbo.UserLoginDetail (UserId, IsActive)
INCLUDE (UserEmail, SetupId, ModifiedDate)
WHERE IsActive = 1;
