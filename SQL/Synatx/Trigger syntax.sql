Create trigger SalesLT.Customertrigger_Suraj
on salesLT.customer
After update, insert  as

--update
update ud set ud.ModifiedDate = Getutcdate() from salesLT.Customer ud
inner join INSERTED i on i.CustomerID = ud.CustomerID

go

Create trigger SalesLT.CustomerAddressTrigger_Suraj
on SalesLT.customerAddress
After update, insert , delete as

-- update 
update C set c.ModifiedDate = getutcdate() from SalesLT.Customer C
inner join inserted U on C.CustomerID=U.customerid

-- deleted
update C set C.ModifiedDate= Getutcdate() from SalesLT.Customer C
inner join deleted d on d.CustomerID=c.CustomerID

go

