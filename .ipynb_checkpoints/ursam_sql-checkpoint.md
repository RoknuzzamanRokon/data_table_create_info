
SELECT * FROM vervotech_hotel_list;

SHOW COLUMNS FROM vervotech_update_data_info;


SELECT COUNT(*) FROM vervotech_update_data_info;

#UPDATE vervotech_mapping SET status = NULL;

SELECT * FROM vervotech_hotel_map_new WHERE Id = 4;

#UPDATE vervotech_hotel_map_new SET status = 'new_data' WHERE Id = 4;

#DELETE FROM vervotech_mapping WHERE Id IN (4516403, 4516402);

SELECT * FROM vervotech_mapping WHERE VervotechId = 39736586;

SELECT COUNT(*) FROM vervotech_hotel_map_update WHERE status = 'new_data';
SELECT COUNT(*) FROM vervotech_hotel_map_update WHERE status = 'new_data';

SELECT COUNT(*) FROM vervotech_mapping WHERE content_update_status = "Done";

SELECT COUNT(*) FROM vervotech_mapping WHERE content_update_status = 'Done';

#UPDATE vervotech_hotel_map_update SET status = 'new_dara' WHERE Id = 8600;
#UPDATE vervotech_hotel_map_update SET ProviderLocationCode = '1415' WHERE Id = 8600;

#ALTER TABLE vervotech_mapping ADD COLUMN hotel_name VARCHAR(255);




#INSERT INTO vervotech_update_data_info (vh_new_total) VALUES (3496);

# Add new created_at column
ALTER TABLE vervotech_update_data_info ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
UPDATE vervotech_hotel_map_update SET created_at = DATE_FORMAT(created_at, '%Y-%m-%d %H:%i');


# Add new ModifiedOn column
ALTER TABLE vervotech_update_data_info ADD COLUMN ModifiedOn TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;



# Find the Last Update Time data
SELECT MAX(created_at) AS last_update_time FROM vervotech_hotel_map_new;

# Show All Rows Updated at the Last Update Time
SELECT * FROM vervotech_hotel_map_new WHERE created_at = (SELECT MAX(created_at) FROM vervotech_hotel_map_new);
SELECT * FROM vervotech_hotel_map_new WHERE ModifiedOn = (SELECT MAX(ModifiedOn) FROM vervotech_hotel_map_new);

# Count the Rows Updated at the Last Update Time
SELECT COUNT(*) FROM vervotech_mapping WHERE created_at = (SELECT MAX(created_at) FROM vervotech_mapping);
SELECT COUNT(*) FROM vervotech_hotel_map_new WHERE ModifiedOn = (SELECT MAX(ModifiedOn) FROM vervotech_hotel_map_new);

SELECT COUNT(*) FROM vervotech_mapping WHERE last_update = (SELECT MAX(last_update) FROM vervotech_mapping);

SELECT COUNT(*) FROM vervotech_mapping WHERE last_update IN (SELECT DISTINCT last_update FROM vervotech_mapping ORDER BY last_update DESC LIMIT 2);

SELECT COUNT(*) 
FROM vervotech_mapping 
WHERE last_update = (SELECT MAX(last_update) FROM vervotech_mapping)
   OR last_update = (SELECT MAX(last_update) FROM vervotech_mapping WHERE last_update < (SELECT MAX(last_update) FROM vervotech_mapping));


SELECT * FROM vervotech_hotel_map_new WHERE ModifiedOn >= NOW() - INTERVAL 25 HOUR;

SELECT COUNT(*) FROM vervotech_hotel_map_new WHERE ModifiedOn >= NOW() - INTERVAL 25 HOUR;


SET time_zone = 'Asia/Dhaka';
SELECT NOW();


SELECT COUNT(*) 
FROM vervotech_hotel_map_update 
WHERE DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') = (
    SELECT DATE_FORMAT(MAX(created_at), '%Y-%m-%d %H:%i') 
    FROM vervotech_hotel_map_update
);



SELECT * FROM vervotech_hotel_map_update WHERE created_at = (SELECT MAX(created_at) FROM vervotech_hotel_map_update);
SELECT COUNT(*) FROM vervotech_hotel_map_update WHERE created_at = (SELECT MAX(created_at) FROM vervotech_hotel_map_update);

SELECT 
    COUNT(CASE WHEN status = 'Skipping data' THEN 1 END) AS skipping_data_count,
    COUNT(CASE WHEN status = 'Update data successful' THEN 1 END) AS update_data_successful_count
FROM 
    vervotech_hotel_map_new 
WHERE 
    created_at = (SELECT MAX(created_at) FROM vervotech_hotel_map_new);


SELECT COUNT(CASE WHEN status = 'Skipping data' THEN 1 END) AS skipping_data_count
    FROM vervotech_hotel_map_new
    WHERE created_at = (SELECT MAX(created_at) FROM vervotech_hotel_map_new);

#
#
#1. vervotech_hotel_list
#2. vervotech_hotel_map_new
#3. vervotech_hotel_map_update
#4. vervotech_mapping
#5. vervotech_update_data_info



# Find last create at file.
SELECT * FROM vervotech_update_data_info ORDER BY created_at DESC LIMIT 1;