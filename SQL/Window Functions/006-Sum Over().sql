/* STEP 1: Identify latest amendment version per Promotion */
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

/* STEP 2: Aggregate committed amount for latest version only */
PromotionInvestmentCTE AS (
    SELECT
        LP.PromotionId,
        LP.SetupId,
        UD.Email,
        LP.CreatedDate,
        SUM(PBA.CommittedAmount) AS CommittedAmount
    FROM LatestPromotion LP
    INNER JOIN PromotionBudgetAllocation PBA
        ON LP.VersionedPromotionId = PBA.VersionedPromotionId
    INNER JOIN UserDetail UD
        ON LP.CreatedBy = UD.UserId
       AND LP.SetupId = UD.SetupId
    WHERE LP.rn = 1  
    GROUP BY
        LP.PromotionId,
        LP.SetupId,
        UD.Email,
        LP.CreatedDate
)

/* STEP 3: Apply SUM() OVER() */
SELECT
    PromotionId,
    SetupId,
    Email,
    CreatedDate,
    CommittedAmount,

    /* Running Total */
    SUM(CommittedAmount) OVER (
        PARTITION BY SetupId, Email
        ORDER BY CreatedDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS RunningCommittedAmount
	,

    /* Total Investment per Setup */
    SUM(CommittedAmount) OVER (
        PARTITION BY SetupId
    ) AS SetupTotalInvestment
	,

    /* Contribution Percentage */
    ROUND(
        CommittedAmount * 100.0 /
        SUM(CommittedAmount) OVER (PARTITION BY SetupId),
        2
    ) AS ContributionPercentage

FROM PromotionInvestmentCTE
ORDER BY SetupId, Email, CreatedDate;
