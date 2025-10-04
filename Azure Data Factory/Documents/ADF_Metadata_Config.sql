CREATE TABLE [dbo].[ADF_Metadata_Config] (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(100),
    SourceSchema NVARCHAR(50),
    DestinationPath NVARCHAR(255),
    IsActive BIT DEFAULT 1
);

INSERT INTO dbo.ADF_Metadata_Config (TableName, SourceSchema, DestinationPath)
VALUES 
('Customer', 'SalesLT', '/raw/customer/'),
('Product', 'SalesLT', '/raw/product/'),
('SalesOrderHeader', 'SalesLT', '/raw/salesorderheader/'),
('SalesOrderDetail', 'SalesLT', '/raw/salesorderdetail/');