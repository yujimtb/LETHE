ALTER TABLE observations
    ALTER COLUMN append_seq DROP DEFAULT;

DROP SEQUENCE IF EXISTS observations_append_seq_seq;
