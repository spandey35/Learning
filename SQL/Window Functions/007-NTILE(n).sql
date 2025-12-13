/* STEP 1: Latest amendment per Promotion */
WITH LatestPromotion AS (
    SELECT
        P.PromotionId,
        P.VersionedPromotionId,
        P.SetupId,
        P.CreatedDate,
        P.CreatedBy,
        P.AmendVersion,
        ROW_NUMBER() OVER (
            PARTITION BY P.PromotionId
            ORDER BY P.AmendVersion DESC
        ) AS rn
    FROM Promotion P
    WHERE P.PromotionStatusId IN (5, 14)
),

/* STEP 2: Aggregate committed investment */
PromotionInvestmentCTE AS (
    SELECT
        LP.PromotionId,
        LP.SetupId,
        LP.CreatedDate,
        SUM(PBA.CommittedAmount) AS CommittedAmount
    FROM LatestPromotion LP
    INNER JOIN PromotionBudgetAllocation PBA
        ON LP.VersionedPromotionId = PBA.VersionedPromotionId
    WHERE LP.rn = 1
    GROUP BY
        LP.PromotionId,
        LP.SetupId,
        LP.CreatedDate
)

/* STEP 3: NTILE segmentation */
SELECT
    PromotionId,
    SetupId,
    CreatedDate,
    CommittedAmount,

    NTILE(4) OVER (
        PARTITION BY SetupId
        ORDER BY CommittedAmount DESC
    ) AS InvestmentQuartile,

    CASE NTILE(4) OVER (
        PARTITION BY SetupId
        ORDER BY CommittedAmount DESC
    )
        WHEN 1 THEN 'Very High Investment'
        WHEN 2 THEN 'High Investment'
        WHEN 3 THEN 'Medium Investment'
        WHEN 4 THEN 'Low Investment'
    END AS InvestmentCategory

FROM PromotionInvestmentCTE
ORDER BY SetupId, InvestmentQuartile, CommittedAmount DESC;
