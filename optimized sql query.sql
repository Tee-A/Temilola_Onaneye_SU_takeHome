SELECT
    f.name,
    f.role,
    SUM(c.hours) AS Total_Tracked_Hours,
    SUM(f.estimated_hours) AS Total_Allocated_Hours
FROM
    raw.clickup_opt c
INNER JOIN
    raw.floatopt f on c.name = f.name
GROUP BY
    f.name, f.role
HAVING
    SUM(c.hours) > 100
ORDER BY
    Total_Allocated_Hours DESC;