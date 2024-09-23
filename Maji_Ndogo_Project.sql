use md_water_services;

/*Analyzing Locations*/
select
	town_name,
    count(town_name) as records_per_town
from
	location
group by town_name;

/*Survey Date Times*/
select	
	min(time_of_record) as survey_start_time,
    max(time_of_record) as survey_end_time,
    timediff(max(time_of_record), min(time_of_record))  survey_duration
from
	visits;
    
/*Average Time in Queue*/
select
	avg(time_in_queue) as avg_queue_time
from
	visits;

/*Hours and Visits*/
select
	time_format(time(time_of_record), '%h:00') as hour_of_day,
	round(avg(nullif(time_in_queue, 0)))  as avg_queue_time
from
	visits
where
	time_in_queue > 0 -- Exclude rows with zero queue time
group by
	hour_of_day 
order by
	hour_of_day;
    
/*Queues per day hour by hour*/
SELECT
	TIME_FORMAT(TIME(time_of_record), '%H:00') AS hour_of_day,
-- Sunday
	ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Sunday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Sunday,
	-- Monday
	ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Monday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Monday,
		-- Tuesday
        ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Tuesday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Tuesday,
		-- Wednesday
        ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Wednesday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Wednesday,
       
        -- Thursday
        ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Thursday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Thursday,
         -- Friday
        ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Friday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Friday,
         -- Saturday
        ROUND(AVG(
		CASE
		WHEN DAYNAME(time_of_record) = 'Saturday' THEN time_in_queue
		ELSE NULL
	END
		),0) AS Saturday
FROM
	visits
WHERE
	time_in_queue != 0 -- this excludes other sources with 0 queue times
GROUP BY
	hour_of_day
ORDER BY
	hour_of_day;
    
/*Days of week per Queue*/
select
	dayname(time_of_record) as day_of_week,
    round(avg(nullif(time_in_queue, 0)))  as avg_queue_time
from
	visits
where
	time_in_queue > 0 -- Exclude rows with zero queue time
group by
	day_of_week;

/*Sunday overview*/
SELECT
	TIME_FORMAT(TIME(time_of_record), '%H:00') AS hour_of_day,
	DAYNAME(time_of_record),
	CASE
		WHEN DAYNAME(time_of_record) = 'Sunday' THEN time_in_queue
		ELSE NULL
	END AS Sunday
FROM
	visits
WHERE
	time_in_queue != 0; -- this exludes other sources with 0 queue times.

/*Join Auditor Report to visits*/
select
	auditor_report.location_id as audit_location,
    auditor_report.true_water_source_score,
    visits.location_id as visit_location,
    visits.record_id
from
	auditor_report
left join
	visits
on auditor_report.location_id = visits.location_id;

select
	auditor_report.location_id,
    visits.record_id,
    auditor_report.true_water_source_score as auditor_score ,
    water_quality.subjective_quality_score as employee_score
from
	auditor_report
left join
	visits
on auditor_report.location_id = visits.location_id
left join
	water_quality
on visits.record_id = water_quality.record_id;

/*Match Analysis*/
select
	auditor_report.location_id,
    visits.record_id,
    auditor_report.true_water_source_score as auditor_score ,
    water_quality.subjective_quality_score as employee_score,
    abs(auditor_report.true_water_source_score - water_quality.subjective_quality_score) as score_difference,
    case when auditor_report.true_water_source_score = water_quality.subjective_quality_score then 'match' else 'mismatch' end as score_match_status
from
	auditor_report
left join
	visits
on auditor_report.location_id = visits.location_id
left join
	water_quality
on visits.record_id = water_quality.record_id;

select
	auditor_report.location_id,
    visits.record_id,
    auditor_report.true_water_source_score as auditor_score ,
    water_quality.subjective_quality_score as employee_score,
    abs(auditor_report.true_water_source_score - water_quality.subjective_quality_score) as score_difference
from
	auditor_report
left join
	visits
on auditor_report.location_id = visits.location_id
left join
	water_quality
on visits.record_id = water_quality.record_id
where auditor_report.true_water_source_score = water_quality.subjective_quality_score;

/*Duplicates Removal*/
select
	auditor_report.location_id,
    visits.record_id,
    auditor_report.true_water_source_score as auditor_score ,
    water_quality.subjective_quality_score as employee_score,
    abs(auditor_report.true_water_source_score - water_quality.subjective_quality_score) as score_difference
from
	auditor_report
left join
	visits
on auditor_report.location_id = visits.location_id
left join
	water_quality
on visits.record_id = water_quality.record_id
where auditor_report.true_water_source_score = water_quality.subjective_quality_score
and visits.visit_count = 1;

/*Incorrect Records*/
SELECT
  auditor_report.*
FROM
  auditor_report
WHERE
  auditor_report.location_id NOT IN (
    SELECT
      auditor_report.location_id
    FROM
      auditor_report
    LEFT JOIN
      visits
    ON auditor_report.location_id = visits.location_id
    LEFT JOIN
      water_quality
    ON visits.record_id = water_quality.record_id
    WHERE
      auditor_report.true_water_source_score = water_quality.subjective_quality_score
    AND
      visits.visit_count = 1
    GROUP BY
      auditor_report.location_id,
      visits.record_id,
      auditor_report.true_water_source_score,
      water_quality.subjective_quality_score,
      ABS(auditor_report.true_water_source_score - water_quality.subjective_quality_score)
    HAVING
      COUNT(*) = 1
  )

/*Incorrect Records part2*/
select
	auditor_report.location_id,
    visits.record_id,
    auditor_report.true_water_source_score as auditor_score ,
    water_quality.subjective_quality_score as employee_score
from
	auditor_report
left join
	visits
on auditor_report.location_id = visits.location_id
left join
	water_quality
on visits.record_id = water_quality.record_id
WHERE
  auditor_report.location_id NOT IN (
    SELECT
      auditor_report.location_id
    FROM
      auditor_report
    LEFT JOIN
      visits
    ON auditor_report.location_id = visits.location_id
    LEFT JOIN
      water_quality
    ON visits.record_id = water_quality.record_id
    WHERE
      auditor_report.true_water_source_score = water_quality.subjective_quality_score
    AND
      visits.visit_count = 1
    GROUP BY
      auditor_report.location_id,
      visits.record_id,
      auditor_report.true_water_source_score,
      water_quality.subjective_quality_score,
      ABS(auditor_report.true_water_source_score - water_quality.subjective_quality_score)
  )
  
  /*Rural Percentage*/
  select count(location_id) as total_count,
	    count(case when location_type = 'rural' then 1 end) as rural_count,
        (count(case when location_type = 'rural' then 1 end) / count(location_id)) * 100 as rural_percentage
from location;

/*The wrong entries*/
select
	auditor_report.location_id as location_id,
	visits.record_id,
    auditor_report.true_water_source_score as auditor_score,
    water_quality.subjective_quality_score as employee_score
from
	auditor_report
join 
	visits
    on auditor_report.location_id =visits.location_id
join
	water_quality
    on water_quality.record_id = visits.record_id
where
	auditor_report.true_water_source_score != water_quality.subjective_quality_score
and
	water_quality.visit_count = 1
    
-- tracing wrong entries by name
select
	auditor_report.location_id as location_id,
	visits.record_id,
	employee.employee_name,
    auditor_report.true_water_source_score as auditor_score,
    water_quality.subjective_quality_score as employee_score
from
	auditor_report
join 
	visits
    on auditor_report.location_id =visits.location_id
join
	employee
    on visits.assigned_employee_id = employee.assigned_employee_id
join
	water_quality
    on water_quality.record_id = visits.record_id
where
	auditor_report.true_water_source_score != water_quality.subjective_quality_score
and
	water_quality.visit_count = 1
    
-- incorrect entries by employee
select
	employee_name,
    count(employee_score) as number_of_mistakes
from
	incorrect_records
group by 
	employee_name;
    
-- view for incorrect records
create view incorrect_records as
select
	auditor_report.location_id as location_id,
	visits.record_id,
	employee.employee_name,
    auditor_report.true_water_source_score as auditor_score,
    water_quality.subjective_quality_score as employee_score
from
	auditor_report
join 
	visits
    on auditor_report.location_id =visits.location_id
join
	employee
    on visits.assigned_employee_id = employee.assigned_employee_id
join
	water_quality
    on water_quality.record_id = visits.record_id
where
	auditor_report.true_water_source_score != water_quality.subjective_quality_score
and
	water_quality.visit_count = 1

-- tracing wrong entries by id
	select
		auditor_report.location_id as location_id,
		visits.record_id,
		visits.assigned_employee_id,
		auditor_report.true_water_source_score as auditor_score,
		water_quality.subjective_quality_score as employee_score
	from
		auditor_report
	join 
		visits
		on auditor_report.location_id =visits.location_id
	join
		water_quality
		on water_quality.record_id = visits.record_id
	where
		auditor_report.true_water_source_score != water_quality.subjective_quality_score
	and
		water_quality.visit_count = 1
        
-- error count
WITH error_count AS ( -- This CTE calculates the number of mistakes each employee made
SELECT
	employee_name,
	COUNT(employee_name) AS number_of_mistakes
FROM
	Incorrect_records
/* Incorrect_records is a view that joins the audit report to the database
for records where the auditor and
employees scores are different*/
GROUP BY
	employee_name)
-- Query
SELECT * FROM error_count;

-- incorrect records view
drop view  if exists incorrect_records;
CREATE VIEW Incorrect_records AS (
SELECT
	auditor_report.location_id,
	visits.record_id,
	employee.employee_name,
	auditor_report.true_water_source_score AS auditor_score,
	wq.subjective_quality_score AS employee_score,
	auditor_report.statements AS statements
FROM
	auditor_report
JOIN
	visits
ON auditor_report.location_id = visits.location_id
JOIN
	water_quality AS wq
ON visits.record_id = wq.record_id
JOIN
	employee
ON employee.assigned_employee_id = visits.assigned_employee_id
WHERE
	visits.visit_count =1
AND auditor_report.true_water_source_score != wq.subjective_quality_score);

-- incorrect records view part2
drop view  if exists incorrect_records;
CREATE VIEW Incorrect_records AS (
SELECT
	auditor_report.location_id,
	visits.record_id,
	employee.employee_name,
	auditor_report.true_water_source_score AS auditor_score,
	wq.subjective_quality_score AS employee_score,
	auditor_report.statements AS statements
FROM
	auditor_report
JOIN
	visits
ON auditor_report.location_id = visits.location_id
JOIN
	water_quality AS wq
ON visits.record_id = wq.record_id
JOIN
	employee
ON employee.assigned_employee_id = visits.assigned_employee_id
WHERE
	visits.visit_count =1
AND auditor_report.true_water_source_score != wq.subjective_quality_score);

-- suspect list view
create view suspect_list as
select
	employee_name,
    number_of_mistakes
from
	error_count
where 
	number_of_mistakes > (SELECT AVG(number_of_mistakes) FROM error_count);
    
-- suspect employee analysis by name
SELECT
	employee_name,
	location_id,
	statements
FROM
	Incorrect_records
WHERE
	employee_name in (SELECT employee_name FROM suspect_list);
    
-- employee with the most mistkes in the job
SELECT
	employee_name,
	number_of_mistakes
FROM
	error_count
WHERE
	number_of_mistakes > 5
order by employee_name asc;

-- average mistakes
select
	avg(number_of_mistakes)
from 
	error_count;
  
-- combined analysis table
SELECT
	water_source.type_of_water_source,
	location.town_name,
	location.province_name,
	location.location_type,
	water_source.number_of_people_served,
	visits.time_in_queue,
	well_pollution.results
FROM
	visits
LEFT JOIN
	well_pollution
ON well_pollution.source_id = visits.source_id
INNER JOIN
	location
ON location.location_id = visits.location_id
INNER JOIN
	water_source
ON water_source.source_id = visits.source_id
WHERE
	visits.visit_count = 1;
    
-- province totals
create view province_totals as
WITH province_totals AS (-- This CTE calculates the population of each province
	SELECT
		province_name,
		SUM(people_served) AS total_ppl_serv
	FROM
		combined_analysis_table
	GROUP BY
		province_name
)
SELECT
	ct.province_name,
	-- These case statements create columns for each type of source.
	-- The results are aggregated and percentages are calculated
	ROUND((SUM(CASE WHEN source_type = 'river'
		THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS river,
	ROUND((SUM(CASE WHEN source_type = 'shared_tap'
		THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS shared_tap,
	ROUND((SUM(CASE WHEN source_type = 'tap_in_home'
		THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS tap_in_home,
	ROUND((SUM(CASE WHEN source_type = 'tap_in_home_broken'
		THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS tap_in_home_broken,
	ROUND((SUM(CASE WHEN source_type = 'well'
		THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS well
FROM
	combined_analysis_table ct
JOIN
	province_totals pt ON ct.province_name = pt.province_name
GROUP BY
	ct.province_name
ORDER BY
	ct.province_name;
    
-- town totals
create view town_totals as
WITH town_totals AS (-- This CTE calculates the population of each town
-- Since there are two Harare towns, we have to group by province_name and town_name
	SELECT province_name, town_name, SUM(people_served) AS total_ppl_serv
	FROM combined_analysis_table
	GROUP BY province_name,town_name
)
SELECT
	ct.province_name,
	ct.town_name,
	ROUND((SUM(CASE WHEN source_type = 'river'
		THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS river,
	ROUND((SUM(CASE WHEN source_type = 'shared_tap'
		THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS shared_tap,
	ROUND((SUM(CASE WHEN source_type = 'tap_in_home'
		THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home,
	ROUND((SUM(CASE WHEN source_type = 'tap_in_home_broken'
		THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home_broken,
	ROUND((SUM(CASE WHEN source_type = 'well'
		THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS well
FROM
	combined_analysis_table ct
JOIN-- Since the town names are not unique, we have to join on a composite key
	town_totals tt ON ct.province_name = tt.province_name AND ct.town_name = tt.town_name
GROUP BY -- We group by province first, then by town.
	ct.province_name,
	ct.town_name
ORDER BY
	ct.town_name;

-- pct broken taps
select
	province_name,
    town_name,
    round(tap_in_home_broken/(tap_in_home_broken + tap_in_home) * 100,0) as Pct_broken_taps
from
	town_totals;

-- project progress
CREATE TABLE Project_progress (
	Project_id SERIAL PRIMARY KEY,
	/* Project_id −− Unique key for sources in case we visit the same
	source more than once in the future.
	*/
	source_id VARCHAR(20) NOT NULL REFERENCES water_source(source_id) ON DELETE CASCADE ON UPDATE CASCADE,
	/* source_id −− Each of the sources we want to improve should exist,
	and should refer to the source table. This ensures data integrity.
	*/
	Address VARCHAR(50), -- Street address
	Town VARCHAR(30),
	Province VARCHAR(30),
	Source_type VARCHAR(50),
	Improvement VARCHAR(50), -- What the engineers should do at that place
	Source_status VARCHAR(50) DEFAULT 'Backlog' CHECK (Source_status IN ('Backlog', 'In progress', 'Complete')),
	/* Source_status −− We want to limit the type of information engineers can give us, so we
	limit Source_status.
	− By DEFAULT all projects are in the "Backlog" which is like a TODO list.
	− CHECK() ensures only those three options will be accepted. This helps to maintain clean data.
	*/
	Date_of_completion DATE, -- Engineers will add this the day the source has been upgraded.
	Comments TEXT -- Engineers can leave comments. We use a TEXT type that has no limit on char length
);

-- water distribution in rural area
select
	round(23740/(15910+23740) *100) as Pct_rural_water_sources;
    
