-- Step 1

drop table if exists #Stg_Temp,#Temp
Create table #Stg_Temp (Userid Bigint , UserEmail Nvarchar(max), Department nvarchar(max), setupid int , ModifiedDate DateTime)
Create table #Temp (Userid Bigint , UserEmail Nvarchar(max), Department nvarchar(max), setupid int , ModifiedDate DateTime)


insert into #Stg_Temp values (1,'Surajss@exmaple.com','IT',1009,GETUTCDATE())
insert into #Stg_Temp values (2,'Sanjay@exmaple.com','HR',1009,GETUTCDATE())
insert into #Stg_Temp values (3,'Shangita@exmaple.com','IT',1009,GETUTCDATE())
insert into #Stg_Temp values (4,'Pandey@exmaple.com','Sales',1009,GETUTCDATE())

--select * from #Stg_Temp
--select * from #Temp


merge into #temp as T
using #stg_temp as s
on t.userid=s.userid
when matched then
	 update set t.ModifiedDate= Getutcdate(),T.UserEmail=S.UserEmail,T.Department=S.Department
When Not Matched then
insert (Userid,UserEmail,Department,setupid,ModifiedDate)
values(s.Userid,s.UserEmail,s.Department,s.setupid,s.ModifiedDate);

select * from #Stg_Temp
select * from #Temp


-- Step 2

truncate table #Stg_temp

insert into #Stg_Temp values (1,'SurajPandey@exmaple.com','IT-1',1009,GETUTCDATE())
insert into #Stg_Temp values (2,'SanjayPandey@exmaple.com','HR-1',1009,GETUTCDATE())
insert into #Stg_Temp values (5,'SanjayPandey@exmaple.com','HR-2',1009,GETUTCDATE())


merge into #temp as T
using #stg_temp as s
on t.userid=s.userid
when matched then
	 update set t.ModifiedDate= Getutcdate(),T.UserEmail=S.UserEmail,T.Department=S.Department
When Not Matched then
insert (Userid,UserEmail,Department,setupid,ModifiedDate)
values(s.Userid,s.UserEmail,s.Department,s.setupid,s.ModifiedDate);

select * from #Stg_Temp
select * from #Temp