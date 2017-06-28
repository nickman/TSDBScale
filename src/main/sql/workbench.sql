/*
select sort_tagpairids_for_kv(array["1", "2", "3", "4", "5"]::numeric[]) from (
  select 
	unnest(tagpairs_for_kv('dc', 'dcx')) as "1",
	unnest(tagpairs_for_kv('host', 'mad-server')) as "2",
	unnest(tagpairs_for_kv('app', 'os-agent')) as "3",
	unnest(tagpairs_for_kv('cpu', '0')) as "4",
	unnest(tagpairs_for_kv('type', '*')) as "5"
) as rx;

select array_agg(sort_tagpairids_for_kv(array["1", "2", "3", "4", "5"]::numeric[])) from (
  select 
	unnest(tagpairs_for_kv('dc', 'dcx')) as "1",
	unnest(tagpairs_for_kv('host', 'mad-server')) as "2",
	unnest(tagpairs_for_kv('app', 'os-agent')) as "3",
	unnest(tagpairs_for_kv('cpu', '0')) as "4",
	unnest(tagpairs_for_kv('type', '*')) as "5"
) as rx;


select tqp from (
select array_agg(sort_tagpairids_for_kv(array["1", "2", "3", "4", "5"]::numeric[])) from (
  select 
	unnest(tagpairs_for_kv('dc', 'dcx')) as "1",
	unnest(tagpairs_for_kv('host', 'mad-server')) as "2",
	unnest(tagpairs_for_kv('app', 'os-agent')) as "3",
	unnest(tagpairs_for_kv('cpu', '0')) as "4",
	unnest(tagpairs_for_kv('type', '*')) as "5"
) as rx) as tqp

select tsd_fqn_tagpair from tsd_fqn_tagpair where tagpair_ids = ANY(
select tqp from (
select array_agg(sort_tagpairids_for_kv(array["1", "2", "3", "4", "5"]::numeric[])) from (
  select 
	unnest(tagpairs_for_kv('dc', 'dcx')) as "1",
	unnest(tagpairs_for_kv('host', 'mad-server')) as "2",
	unnest(tagpairs_for_kv('app', 'os-agent')) as "3",
	unnest(tagpairs_for_kv('cpu', '0')) as "4",
	unnest(tagpairs_for_kv('type', '*')) as "5"
) as rx) as tqp)




select tqp, fqn_tp_id from (
select array_agg(sort_tagpairids_for_kv(array["1", "2", "3", "4", "5"]::numeric[])) from (
  select 
	unnest(tagpairs_for_kv('dc', 'dcx')) as "1",
	unnest(tagpairs_for_kv('host', 'mad-server')) as "2",
	unnest(tagpairs_for_kv('app', 'os-agent')) as "3",
	unnest(tagpairs_for_kv('cpu', '0')) as "4",
	unnest(tagpairs_for_kv('type', '*')) as "5"
) as rx) as tqp, tsd_fqn_tagpair b
where tagpair_ids = ANY(tqp)




*/


CREATE OR REPLACE FUNCTION test_dynamic()
  RETURNS NUMERIC[][] AS
$BODY$
DECLARE
  tagpair_ids NUMERIC[];
  key_pattern TEXT = replace(key, '*', '%');
  value_pattern TEXT = replace(value, '*', '%');
BEGIN
  select array_agg(distinct tagpair_id) 
  into tagpair_ids 
  from tsd_tagpair t, tsd_tagk k, tsd_tagv v 
  where t.tagk_id = k.tagk_id 
  and t.tagv_id = v.tagv_id 
  and k.name like key_pattern 
  and v.name like value_pattern;
  return tagpair_ids;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

