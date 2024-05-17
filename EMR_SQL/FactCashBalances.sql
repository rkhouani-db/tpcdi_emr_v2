INSERT INTO {wh_db}_{scale_factor}.FactCashBalances
SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  bigint(date_format(datevalue, 'yyyyMMdd')) sk_dateid,
  cash,
  c.batchid
FROM {wh_db}_{scale_factor}_stage.v_CashTransactionIncremental c 
JOIN {wh_db}_{scale_factor}.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate;