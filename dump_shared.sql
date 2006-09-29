CREATE OR REPLACE FUNCTION dump_shared()
RETURNS VOID
LANGUAGE plperlU
AS $$
use YAML;
elog NOTICE, Dump(\%_SHARED);
return;
$$;
