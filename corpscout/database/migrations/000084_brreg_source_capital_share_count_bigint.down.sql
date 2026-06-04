ALTER TABLE brreg_source.capital
  ALTER COLUMN share_count TYPE INTEGER
  USING CASE
    WHEN share_count BETWEEN 0 AND 2147483647 THEN share_count::integer
    ELSE NULL
  END;
