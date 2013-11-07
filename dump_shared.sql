CREATE OR REPLACE FUNCTION dump_shared()
RETURNS VOID
LANGUAGE plperlU
AS $$
use JSON;
elog NOTICE, json_encode(\%_SHARED);
return;
$$;
