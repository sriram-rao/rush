CREATE TABLE pipeline( 
	id SERIAL PRIMARY KEY, 
	name VARCHAR(200),
	definition JSON
);
