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



select tsd_id_arr(jsonb('[{"metric":"sys.cpu", "tags" : {"dc":"lga","app":"edge-web","host":"web01"}}, {"metric":"sys.cpu", "tags" : {"dc":"dal3","app":"edge-web","host":"web01"}}]'))

select clean()

select * from tsd_tagk

select * from tsd_tagpair


select get_tagpair_ids(jsonb('{"dc":"lga","app":"edge-web","host":"web01"}'))

select currval('TAGK_SEQ')  --387, 395, 403

insert into tsd_tagk (tagk_id, version, name)
select nextval('TAGK_SEQ'), 1, unnest(array['foo', 'bar', 'baz'])
where not exists (
	select * from tsd_tagk where name = ANY(array['foo', 'bar', 'baz'])
)

  insert into tsd_tagv (tagv_id, version, name)
  select nextval('TAGV_SEQ'), 1, unnest(array['lga', 'edge-web', 'web01'])
  where not exists (
    select * from tsd_tagv where name IN (SELECT unnest(array['lga', 'edge-web', 'web01']))
  );

select 
	(select array_agg(tagk_id) from tsd_tagk where name = ANY(array['dc', 'app', 'host'])),
	(select array_agg(tagv_id) from tsd_tagv where name = ANY(array['lga', 'edge-web', 'web01']))


select jsonb_object(a,b) from (select 
	(select jsonb_build_object(name, tagk_id) as a from tsd_tagk where name = ANY(array['dc', 'app', 'host'])),
	(select jsonb_build_object(name, tagv_id) as b from tsd_tagv where name = ANY(array['lga', 'edge-web', 'web01']))
) as c	


select jsonb_build_object(
	(select array_agg(jsonb_build_object(name, tagk_id)) as a from tsd_tagk where name = ANY(array['dc', 'app', 'host'])),
	(select array_agg(jsonb_build_object(name, tagk_id)) as a from tsd_tagk where name = ANY(array['dc', 'app', 'host']))
)	

/*
jsonb_insert(target jsonb, path text[], new_value jsonb, [insert_after boolean])
*/

select jsonb_insert(jsonb('{"keys" : {}, "values" : {}}'), array['keys', 'aaa'], jsonb('1'))

select 
	(select jsonb_insert(jsonb('{"keys" : {}, "values" : {}}'), array['keys', name], jsonb(tagk_id::text)) from tsd_tagk where name = ANY(array['dc', 'app', 'host'])),
	(select jsonb_insert(jsonb('{"keys" : {}, "values" : {}}'), array['values', name], jsonb(tagv_id::text)) from tsd_tagv where name = ANY(array['lga', 'edge-web', 'web01']))



select jsonb_build_object(name, tagk_id) as a from tsd_tagk where name = ANY(array['dc', 'app', 'host'])

select jsonb_build_object(name, tagk_id) as a from tsd_tagk where name = ANY(array['dc', 'app', 'host'])
	

	


select array_agg(x) from (select jsonb_object_keys(jsonb('{"dc":"lga","app":"edge-web","host":"web01"}')) as x) as y


select * from jsonb_each_text(jsonb('{"dc":"lga","app":"edge-web","host":"web01"}'))

select array_agg(key) as keys, array_agg(value) as values from jsonb_each_text(jsonb('{"dc":"lga","app":"edge-web","host":"web01"}'))



select unnest(array[404,405,406]) tid, unnest(array[22610,22611,22612]) vid

select nextval('TAGPAIR_SEQ'), unnest(array[404,405,406]), unnest(array[22610,22611,22612])

--  as x(tagk_id numeric, tagv_id numeric)

 select * from json_populate_record(null::tsd_tagpair, '{"tagk_id":[404,405,406] ,"tagv_id":[22610,22611,22612]}') 

 select * from json_array_elements_text('[[404,405,406], [22610,22611,22612]]') 


 select * from json_each_text('{"tagk_id":[404,405,406] ,"tagv_id":[22610,22611,22612]}') 

	insert into tsd_tagpair values (0, 404, 22610)
	insert into tsd_tagpair values (1, 405, 22611)
	insert into tsd_tagpair values (2, 406, 22612)

	
	  insert into tsd_tagpair  (tagpair_id, tagk_id, tagv_id)
  select nextval('TAGPAIR_SEQ'), unnest(array[404,405,406]), unnest(array[22610,22611,22612])
  where not exists (
	select tagpair_id from tsd_tagpair t, (select unnest(array[404,405,406]) tid, unnest(array[22610,22611,22612]) vid) q
	where t.tagk_id = q.tid
	and t.tagv_id = q.vid
  );

select array_to_json(array_agg(row_to_json(t))) from (
 	select tagpair_id pairid, k.tagk_id, k.name kname, v.tagv_id, v.name vname from tsd_tagpair t, (select unnest(array[407,408,409]) tid, unnest(array[22614,22615,22616]) vid) q,
 	tsd_tagk k, tsd_tagv v
	where t.tagk_id = q.tid
	and t.tagv_id = q.vid
	and t.tagv_id = v.tagv_id
	and t.tagk_id = k.tagk_id
) t


select row_to_json(x) from (
select pairid, row_to_json(t) from (
 	select tagpair_id pairid, k.tagk_id, k.name kname, v.tagv_id, v.name vname from tsd_tagpair t, (select unnest(array[407,408,409]) tid, unnest(array[22614,22615,22616]) vid) q,
 	tsd_tagk k, tsd_tagv v
	where t.tagk_id = q.tid
	and t.tagv_id = q.vid
	and t.tagv_id = v.tagv_id
	and t.tagk_id = k.tagk_id
) t ) x


select json_build_object(pairid, row_to_json(t)) from (
 	select tagpair_id pairid, k.tagk_id, k.name kname, v.tagv_id, v.name vname from tsd_tagpair t, (select unnest(array[407,408,409]) tid, unnest(array[22614,22615,22616]) vid) q,
 	tsd_tagk k, tsd_tagv v
	where t.tagk_id = q.tid
	and t.tagv_id = q.vid
	and t.tagv_id = v.tagv_id
	and t.tagk_id = k.tagk_id
) t 

select array_agg(json_build_object(pairid, row_to_json(t))) from (
 	select tagpair_id pairid, k.tagk_id, k.name kname, v.tagv_id, v.name vname from tsd_tagpair t, (select unnest(array[407,408,409]) tid, unnest(array[22614,22615,22616]) vid) q,
 	tsd_tagk k, tsd_tagv v
	where t.tagk_id = q.tid
	and t.tagv_id = q.vid
	and t.tagv_id = v.tagv_id
	and t.tagk_id = k.tagk_id
) t 


select json_object_agg(pairid, row_to_json(t)) from (
 	select tagpair_id pairid, k.tagk_id, k.name kname, v.tagv_id, v.name vname from tsd_tagpair t, (select unnest(array[407,408,409]) tid, unnest(array[22614,22615,22616]) vid) q,
 	tsd_tagk k, tsd_tagv v
	where t.tagk_id = q.tid
	and t.tagv_id = q.vid
	and t.tagv_id = v.tagv_id
	and t.tagk_id = k.tagk_id
) t



select tsd_id_for_ids(jsonb('[{"timestamp":1499016673168,"metric":"sys.net.client","value":1,"tags":{"protocol":"udp","state":"established","port":"55499","address":"0:0:0:0:0:0:0:1","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673168,"metric":"sys.net.client","value":1,"tags":{"protocol":"udp","state":"established","port":"59066","address":"0:0:0:0:0:0:0:1","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673168,"metric":"sys.net.client","value":2,"tags":{"protocol":"tcp","state":"time_wait","port":"631","address":"0:0:0:0:0:0:0:1","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673168,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"104.16.221.173","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673168,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"104.16.223.173","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673168,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"104.20.86.245","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673168,"metric":"sys.net.client","value":6,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"104.28.31.132","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673169,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"104.31.68.134","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673170,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"443","address":"172.217.6.238","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673170,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"443","address":"173.194.205.157","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673171,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"208.88.225.39","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673171,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"time_wait","port":"80","address":"216.21.13.10","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673171,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"syn_sent","port":"52753","address":"34.227.9.64","dc":"dcy","host":"mad-server","app":"os-agent"}},{"timestamp":1499016673172,"metric":"sys.net.client","value":1,"tags":{"protocol":"tcp","state":"syn_sent","port":"36129","address":"78.15.191.215","dc":"dcy","host":"mad-server","app":"os-agent"}}]'))


  select unnest(array[1,2,3]), unnest(array[4,5,6]), unnest(array[7,8,9])


  select array_agg(p) from (
  	select to_timestamp(x/1000) as p from (
		select unnest(array[1499014152593, 1499014152594, 1499014152595]::double precision[]) as x
     ) as y
  ) as t


  select * from tsd_tsmeta where fqn like '%elapsed%'


  select avg(value::int)::int from tsdb where fqnid = 28999

  select * from tsd_metric where name like '%elapsed%'