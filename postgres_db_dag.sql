CREATE TABLE iris(
	iris_id SERIAL PRIMARY KEY,
	iris_sepal_length REAL,
	iris_sepal_width REAL,
	iris_petal_length REAL,
	iris_petal_width REAL,
	iris_variety VARCHAR(16)
);

COPY iris(iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety)
FROM '/home/vdp/Documents/iris.csv'
DELIMITER ','
CSV HEADER;

select * from iris;

CREATE TABLE iris_tgt AS (
	SELECT
		iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety
	FROM iris
	WHERE 1 = 2
);

select * from iris_tgt;
truncate iris_tgt;