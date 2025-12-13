with TopPromotion as (
Select P.PromotionId, P.Name, P.StartDate, P.EndDate,PIN.InvestmentTypeId, Sum(PBA.CommittedAmount) As CommittedAmount, P.Setupid
from Promotion  P
Inner join PromotionInvestment PIN on P.VersionedPromotionId = PIN.VersionedPromotionId
inner join PromotionBudgetAllocation PBA on P.VersionedPromotionId = PBA.VersionedPromotionId
where year(P.CreatedDate)=2025 and P.PromotionStatusId = 5
group by P.PromotionId, P.Name, P.StartDate, P.EndDate,PIN.InvestmentTypeId,  P.Setupid)

select * from (
select *,Rank() over (Partition By Setupid order By CommittedAmount desc) as RNK
from TopPromotion P) T 
where T.RNk <2 order by CommittedAmount desc 

