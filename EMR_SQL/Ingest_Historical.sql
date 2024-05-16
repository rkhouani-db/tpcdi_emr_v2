INSERT INTO {wh_db}_{scale_factor}.{table}
SELECT {tgt_query}
FROM
  (
    SELECT
      split(value, "[|]") val
    FROM
      text.`{tpcdi_directory}sf={scale_factor}/{path}/{filename}`
  );
