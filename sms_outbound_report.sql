with fqdnq as
(
	select (case when ('@all'::text like '@%') then 'all' else '@all' end) as fqdnn
),

qtargetdate as (
	select (case when ( '@dateto'::text like '@%' ) then now()::text else '@dateto'::text end)::date as targetdate,
			((case when ( '@dateto'::text like '@%' ) then now()::text else '@dateto'::text end)::date + '1 day'::interval)::date as targetdateplusone
),

tmp_sms as 
(
	select q.*
	from
	(
		select a.entity->>'fqdn' as fqdn, soc.entity->>'name' as sms_driver_name, q.inserted::timestamp(0) as inserted, 
				date_trunc('hour', inserted) as "hoursent", q.destination, q.successful, q.error, q.service_task_request_fk
		from
		(
			select r.id as _resultid, r.inserted, 
				r.entity->>'successful'as successful, 
				r.entity->>'error' as error,
				(jsonb_array_elements((r.entity->'result')->'transferReports')->>'smsConfigurationId')::uuid as sms_outbound_configuration_fk,
				''''::text||(jsonb_array_elements((r.entity->'result')->'transferReports')->>'destination') as destination,
				r.service_task_request_fk
			from service_task_result r
			where r.inserted > ((select targetdate from qtargetdate))::timestamp without time zone 
					and r.inserted < ((select targetdateplusone from qtargetdate))::timestamp without time zone 
					and r.entity->>'entityType' = 'smsOutboundTransferResult'
		) q
			join sms_outbound_configuration soc on soc.id = q.sms_outbound_configuration_fk
			join app a on CAST (soc.entity->>'appId' AS UUID) = a.id
	) q
	where (q.fqdn = (select fqdnn from fqdnq) or 
			('all' = (select fqdnn from fqdnq) and 'amrefsms.mezzanineware.com' != q.fqdn))
),

smsdetailsummary as 
(
	select q.successful, q.fqdn, q.sms_driver_name, q.hoursent, count(q.*)::integer as sms_sent_counter
	from
	(
		select * from tmp_sms
	) q
	group by q.successful, q.fqdn, q.sms_driver_name, q.hoursent
	order by q.successful, q.fqdn, q.sms_driver_name, q.hoursent
),

tmp_sms_with_text as
(
	select tm.fqdn, tm.sms_driver_name, tm.inserted, tm.hoursent, tm.destination, tm.successful, tm.error,
			re.entity->'task'->>'text' as messagetext
	from tmp_sms tm
			join service_task_request re on re.id = tm.service_task_request_fk
	where tm.fqdn != 'amrefsms.mezzanineware.com'
),

tmp_sms_report as
(
	select * from tmp_sms_with_text order by fqdn, inserted
),

tmp_sms_report_failures as
(
	select * from tmp_sms_with_text where successful != 'true' order by fqdn, inserted
)
select * from smsdetailsummary
/*0smscount select count(*) from tmp_sms; 0smscount*/
/*1smssummary select * from smsdetailsummary; 1smssummary*/
/*2smsfailuresdetail select * from tmp_sms_report_failures; 2smsfailuresdetail*/
/*3smsdetail select * from tmp_sms_report; 3smsdetail*/