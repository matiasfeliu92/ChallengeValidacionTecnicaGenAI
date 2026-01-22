SELECT * FROM public.raw_logs;

SELECT 
	data->>'event_id' AS event_id, 
	data->>'timestamp' AS timestamp, 
	data->>'event_type' AS event_type, 
	data->>'user_id' AS user_id, 
	data->>'document_id' AS document_id
FROM 
	public.raw_logs;


CREATE TABLE IF NOT EXISTS Events AS (
	SELECT 
		data->>'event_id' AS event_id, 
		data->>'timestamp' AS timestamp, 
		data->>'event_type' AS event_type, 
		data->>'user_id' AS user_id, 
		data->>'document_id' AS document_id
	FROM 
		public.raw_logs
);

CREATE TABLE OR REPLACE IF NOT EXISTS Comments AS (
	SELECT 
		data->>'event_id' AS event_id,
		data->>'comment_text' AS comment_text 
	FROM 
		public.raw_logs
	WHERE
		data->>'event_type' = 'comment_added'
);

CREATE TABLE IF NOT EXISTS Edits AS (
	SELECT 
		data->>'event_id' AS event_id, 
		data->>'edit_length' AS edit_length 
	FROM 
		public.raw_logs
	WHERE
		data->>'event_type' = 'document_edit'
);

CREATE TABLE IF NOT EXISTS Shares AS (
	SELECT 
		data->>'event_id' AS event_id, 
		data->>'shared_with' AS shared_with 
	FROM 
		public.raw_logs
	WHERE
		data->>'event_type' = 'document_shared'
)