CREATE OR REPLACE FUNCTION scrub_shared()
RETURNS VOID
LANGUAGE plperlu
AS $$
    %_SHARED = ();
    return;
$$;
