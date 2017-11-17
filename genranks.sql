UPDATE ny.pm_bottlenecks as pmb1
SET phed_rank = q.phed_rank,
    tttr_rank = q.tttr_rank,
    lottr_rank= q.lottr_rank
    FROM (select tmc, year, month,
         rank() OVER (PARTITION BY year, month ORDER BY phed DESC) as phed_rank,
         rank() OVER (PARTITION BY year, month ORDER BY tttr DESC) as tttr_rank,
         rank() OVER (PARTITION BY year, month ORDER BY lottr DESC) as lottr_rank
         from ny.pm_bottlenecks) as q
    WHERE pmb1.tmc=q.tmc AND pmb1.year=q.year AND pmb1.month=q.month;
