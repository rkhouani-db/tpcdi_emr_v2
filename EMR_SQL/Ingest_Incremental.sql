INSERT INTO {tgt_db}.{table}
SELECT * FROM {wh_db}_{scale_factor}_stage.{view};
