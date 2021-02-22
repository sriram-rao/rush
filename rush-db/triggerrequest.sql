CREATE TABLE triggerrequest (
	id SERIAL PRIMARY KEY, 
	pipeline varchar(200), 
	job varchar(200), 
	jobinstance int, 
	status varchar(20), 
	inserttime TIMESTAMP);