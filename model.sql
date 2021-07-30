WITH jids AS
  (SELECT *
   FROM public.jobs
   WHERE id in
       (SELECT job_id
        FROM public.tags
        WHERE name = 'client')),
     units AS
  (SELECT *
   FROM public.units
   WHERE job_id in
       (SELECT id
        FROM jids)),
     job AS
  (SELECT name,
          job_id
   FROM public.tags
   WHERE name ilike '1'
     OR name ilike '2'
     OR name ilike '3'
     OR name ilike '4'
     OR name ilike '5'),
     doc_type AS
  (SELECT name,
          job_id
   FROM public.tags
   WHERE name ilike '1099%'
     OR name ilike '1040%'
     OR name ilike 'federal%'
     OR name ilike 'state%'
     OR name ilike 'w%'
     OR name ilike 'po'
     OR name ilike 'bol'
     OR name ilike 'invoice'
     OR name ilike 'utility'
     OR name ilike 'all'),
     batch AS
  (SELECT name,
          job_id
   FROM public.tags
   WHERE name ilike 'sample'
     OR name ilike '%-2020_Batch_%'
     OR name ilike '%-2021_Batch_%')
SELECT jids.id,
       title,
       jids.user_id,
       CASE
           WHEN (jids.state = 1) THEN 'unordered'
           WHEN (jids.state = 2) THEN 'running'
           WHEN (jids.state = 3) THEN 'paused'
           WHEN (jids.state = 4) THEN 'canceled'
           WHEN (jids.state = 5) THEN 'finished'
           WHEN (jids.state = 6) THEN 'locked_out'
           WHEN (jids.state = 7) THEN 'data_deleted'
           WHEN (jids.state = 8) THEN 'archiving'
           WHEN (jids.state = 9) THEN 'archived'
           WHEN (jids.state = 10) THEN 'launching'
       END AS state,
       COUNT(units.id) AS unit_count,
       job.name AS job,
       doc_type.name AS doc_type,
       batch.name AS batch,
       jids.created_at
FROM jids
LEFT JOIN job
  ON jids.id = job.job_id
LEFT JOIN doc_type
  ON jids.id = doc_type.job_id
LEFT JOIN batch
  ON jids.id = batch.job_id
LEFT JOIN units
  ON jids.id = units.job_id
WHERE job is NOT NULL
GROUP BY jids.id,
         title,
         jids.created_at,
         jids.state,
         jids.user_id,
         job,
         doc_type,
         batch
ORDER BY batch,
         job,
         doc_type,
         created_at DESC