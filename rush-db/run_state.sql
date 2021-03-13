CREATE TABLE run_state (
	id SERIAL PRIMARY KEY, 
	pipeline varchar(200), 
	job varchar(200), 
	jobinstance int, 
	retry int,
	status varchar(20), 
	worker varchar(100), 
	readytime TIMESTAMP, 
	starttime TIMESTAMP, 
	endtime TIMESTAMP,
	message VARCHAR(1000));