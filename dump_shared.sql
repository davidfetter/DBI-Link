CREATE OR REPLACE FUNCTION dump_shared()
RETURNS VOID
LANGUAGE plperlU
AS $$
use JSON;
elog NOTICE, to_json(\%_SHARED);
return;
$$;
