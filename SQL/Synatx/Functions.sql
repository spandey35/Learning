--------------------
-- Scalar fucntion
--------------------

IF EXISTS (SELECT * FROM sys.objects  WHERE name = 'SurajFunctionTest'  AND type = 'FN')
BEGIN
    DROP FUNCTION dbo.SurajTestScalarFunction;
END
GO

CREATE FUNCTION dbo.SurajTestScalarFunction (@Productid INT)
RETURNS NVARCHAR(MAX)
AS 
BEGIN 
    DECLARE @ProductDetails NVARCHAR(MAX);

    SELECT @ProductDetails = Name + '-' + ProductNumber + '-' + Color
    FROM SalesLT.Product
    WHERE ProductID = @Productid;

    RETURN @ProductDetails;
END
GO

-- Calling Scalar Function
select dbo.SurajTestScalarFunction(680)
select dbo.SurajTestScalarFunction(Productid),* from SalesLT.Product



----------------------
-- Table Function
-----------------------


If EXISTS (select * from sys.objects where object_id = object_ID(N'[dbo].[SurajTestTableFunction]') and type = 'FN')
BEGIN
    drop function [dbo].[SurajTestTableFunction];
END
GO

Create fu ction [dbo].[SurajTestTableFunction](@ProductCategory Nvarchar(max) , @Productid int)
Returns Table
As 
    Return (

    Select P.ProductID,P.ProductNumber,P.Color,P.ListPrice,PC.Name as [ProductCategory]
    from SalesLT.product P with(nolock)
    inner join SalesLT.ProductCategory PC with(nolock) on P.ProductCategoryId = PC.ProductCategoryID
    where PC.Name= @ProductCategory and P.ProductID=@Productid
)

-- Calling a Table function
select * from dbo.SurajTestTableFunction('Helmets',707)