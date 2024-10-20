SELECT
c.name,
f.role,
SUM(c.hours) AS Total_Tracked_Hours
SUM(f.estimated_hours) AS Total_Allocated_Hours,
date
FROM
raw.clickup c
JOIN
raw.float f on c.name = f.name
GROUP BY
c.name, f.role
HAVING
SUM(c.hours) > 100
ORDER BY
Total_Allocated_Hours DESC;

-- This query was corrected to form the query below:

SELECT
c.name,
f.role,
SUM(c.hours) AS Total_Tracked_Hours,
SUM(f.estimated_hours) AS Total_Allocated_Hours
FROM
raw.clickup c
JOIN
raw.float f on c.name = f.name
GROUP BY
c.name, f.role
HAVING
SUM(c.hours) > 100
ORDER BY
Total_Allocated_Hours DESC;

-- Added a comma in line 4. Removed the date column in line 6 because it wasn't used in the group by and then removed trailing comma in line 5.


