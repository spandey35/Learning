Create table #Source
(CustomerID   INT,FullName VARCHAR(100),City VARCHAR(50),Phone VARCHAR(20),Email VARCHAR(100));

Create table #Target
( CustomerKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
  CustomerID INT,                           -- Business Key
  FullName VARCHAR(100), City  VARCHAR(50),Phone VARCHAR(20),Email VARCHAR(100),StartDate DATETIME,EndDate   DATETIME,
  IsCurrent BIT
);



-- Day 1 data coming from source
INSERT INTO #Source VALUES
(101, 'Rahul Sharma', 'Mumbai', '9990001111', 'rahul@gmail.com'),
(102, 'Amit Verma',   'Delhi',  '9990002222', 'amit@gmail.com'),
(103, 'Neha Singh',   'Pune',   '9990003333', 'neha@gmail.com');


INSERT INTO #Target (CustomerID, FullName, City, Phone, Email, StartDate, EndDate, IsCurrent)
SELECT CustomerID, FullName, City, Phone, Email,
       GETDATE(), '9999-12-31', 1
FROM #Source;

Truncate table #Source 


-- Day 2 data
-- Customer 102 city changed Delhi → Noida
-- Customer 103 phone changed
-- Customer 104 is new customer
INSERT INTO #Source VALUES
(101, 'Rahul Sharma', 'Mumbai', '9990001111', 'rahul@gmail.com'),
(102, 'Amit Verma',   'Noida',  '9990002222', 'amit@gmail.com'),
(103, 'Neha Singh',   'Pune',   '8887773333', 'neha@gmail.com'),
(104, 'Rohit Jain',   'Bangalore','7778889999','rohit@gmail.com');


update T set t.IsCurrent=0 from #Target T 
inner join #Source S on T.CustomerID=S.CustomerID
where T.IsCurrent = 1 And(
ISNULL(S.FullName, '') <> ISNULL(T.FullName, '') OR
isnull(S.City,'') <> isnull(T.City,'') OR
isnull(S.Email,'') <> isnull(T.Email, '') OR
isnull(S.Phone, '')<> isnull(T.Phone, '')
);


insert into #Target (CustomerID, FullName, City, Phone, Email, StartDate, EndDate, IsCurrent)
select S.CustomerID, S.FullName, S.City, S.Phone, S.Email,
       GETDATE(), '9999-12-31', 1 from 
#Source S 
left join #Target T  ON S.CustomerID = T.CustomerID
and T.IsCurrent=1
where S.CustomerID is null OR (
ISNULL(S.FullName, '') <> ISNULL(T.FullName, '') OR
isnull(S.City,'') <> isnull(T.City,'') OR
isnull(S.Email,'') <> isnull(T.Email, '') OR
isnull(S.Phone, '')<> isnull(T.Phone, ''));


select * from #Target

