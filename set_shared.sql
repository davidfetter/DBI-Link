CREATE OR REPLACE FUNCTION set_shared(in_name TEXT, in_val TEXT)
RETURNS VOID
LANGUAGE plperlu
AS $$
    $_SHARED{$_[0]} = $_[1];
    return;
$$;
