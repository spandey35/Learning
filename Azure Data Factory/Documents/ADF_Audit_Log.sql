CREATE TABLE [dbo].[ADF_Audit_Log] (
    RunID INT IDENTITY(1,1),
    TableName NVARCHAR(100),
    Status NVARCHAR(50),
    StartTime DATETIME,
    EndTime DATETIME,
    RowsCopied INT,
    Message NVARCHAR(MAX)
);
