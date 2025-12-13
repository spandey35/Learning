-- 4.LAG() — For each SetupId and PromotionId, calculate 
--			the difference in committed investment compared to the previous promotion (by CreatedBy).

WITH LatestPromotionVersion AS (
SELECT P.PromotionId,P.PromotionStatusId,P.VersionedPromotionId,P.SetupId,P.CreatedDate,P.AmendVersion,
P.StartDate, P.EndDate, UD.Email,
ROW_NUMBER() OVER ( PARTITION BY P.PromotionId  ORDER BY P.AmendVersion ) AS AmendRank, 
Sum(PBA.CommittedAmount) as CommittedAmount
FROM Promotion P
Inner join PromotionInvestment PIN On P.VersionedPromotionId = PIN.VersionedPromotionId
Inner join PromotionBudgetAllocation PBA ON  P.VersionedPromotionId = PBA.VersionedPromotionId 
inner join UserDetail UD on P.CreatedBy = UD.CreatedBy
WHERE YEAR(P.CreatedDate) = 2025
AND MONTH(P.CreatedDate) BETWEEN 10 AND 12
AND P.PromotionStatusId IN (5, 14)
Group by P.PromotionId,p.PromotionStatusId,P.VersionedPromotionId,P.SetupId,P.CreatedDate,P.AmendVersion,
P.StartDate, P.EndDate, UD.Email
)

SELECT
    PromotionId,
    PromotionStatusId,
    AmendVersion,
    SetupId,
    Email,
    CommittedAmount,
    LAG(CommittedAmount) OVER (
        PARTITION BY SetupId, Email
        ORDER BY amendversion
    ) AS PreviousCommittedAmount,
    CommittedAmount
        - LAG(CommittedAmount) OVER (
            PARTITION BY SetupId, Email
            ORDER BY amendversion
        ) AS DiffInvestment
FROM LatestPromotionVersion
ORDER BY SetupId, Email, CreatedDate;



