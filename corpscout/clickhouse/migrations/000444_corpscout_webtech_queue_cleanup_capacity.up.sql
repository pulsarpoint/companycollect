-- Queue cleanup must not wait for 20 free slots while unrelated tables merge.
-- Keep one free slot as the admission threshold for this input table only.
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.webtech_scan_input
    MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 1;
