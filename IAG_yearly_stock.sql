CREATE VIEW IAG_yearly_stock AS

WITH temp_IAG_history AS
(SELECT *
, EXTRACT(year FROM date) AS year
FROM public."IAG.L_history"
)
SELECT
year
, AVG(open) AS avg_open
, AVG(close) AS avg_close
, AVG(high) AS avg_high
, AVG(low) AS avg_low
FROM temp_IAG_history
group by(year);