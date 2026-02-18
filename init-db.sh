#!/bin/bash
set -e

echo "Creating and seeding records table..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'

CREATE TABLE IF NOT EXISTS public.records (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name VARCHAR(255) NOT NULL,
    value DECIMAL(18,4) NOT NULL,
    metadata JSONB NOT NULL
);

-- check if table is already seeded
DO $$
DECLARE
    rec_count bigint;
BEGIN
    SELECT COUNT(*) INTO rec_count FROM public.records;
    IF rec_count = 0 THEN
        RAISE NOTICE 'Seeding 10 million records...';
        INSERT INTO public.records (name, value, metadata)
        SELECT
            md5(random()::text || i::text) AS name,
            (random() * 1000)::numeric(18,4) AS value,
            jsonb_build_object('nested', jsonb_build_object('a', random(), 'b', md5((random()::text || i::text))))
        FROM generate_series(1, 10000000) i;
        RAISE NOTICE 'Seeding complete';
    ELSE
        RAISE NOTICE 'Table already seeded with % records', rec_count;
    END IF;
END$$;

EOSQL

echo "Seed script completed."
