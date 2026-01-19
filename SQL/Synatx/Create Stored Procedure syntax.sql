Create Procedure dbo.TestProcedure (@Userid bigint)
As Begin 

Set NoCount  On ;

Select LoginId,UserEmail,CreatedDate,ModifiedDate,IsActive,SetupId 
	from UserLoginDetail 
		where userid = @Userid

End