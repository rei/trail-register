create table usages (
	app varchar(20), 
	env varchar(20), 
	category varchar(30), 
	[key] varchar(300), 
	date int, 
	num int,
	primary key (app, env, category, [key], date)
)