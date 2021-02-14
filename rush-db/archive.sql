CREATE TABLE runningstate (
	id int PRIMARY KEY, 
	pipeline varchar(200), 
	job varchar(200), 
	jobinstance int, 
	retry int,
	status varchar(20), 
	worker varchar(100), 
	starttime TIMESTAMP, 
	endtime TIMESTAMP);