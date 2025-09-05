CREATE DATABASE sample_news_events_db;
USE sample_news_events_db;
IF OBJECT_ID('events_masters_final', 'U') IS NOT NULL
    DROP TABLE events_masters_final;
GO


CREATE TABLE events_masters_final (
    event_id NVARCHAR(50) PRIMARY KEY,
    event_type NVARCHAR(MAX),
    summary NVARCHAR(MAX),
    category NVARCHAR(MAX),
    event_date NVARCHAR(MAX),
    confidence DECIMAL(5,4),
    company1_id NVARCHAR(MAX),
    company2_id NVARCHAR(MAX),
    company1_name NVARCHAR(MAX),
    company1_domain NVARCHAR(MAX),
    company1_ticker NVARCHAR(MAX),
    company2_name NVARCHAR(MAX),
    company2_domain NVARCHAR(MAX),
    company2_ticker NVARCHAR(MAX)
);
GO

SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE='BASE TABLE';

-- Check row counts for each table
SELECT t.name AS table_name, SUM(p.rows) AS total_rows
FROM sys.tables t
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0,1)
GROUP BY t.name
ORDER BY total_rows DESC;


SELECT DISTINCT *
INTO temp_final_dataset_part1
FROM final_dataset_part1;

TRUNCATE TABLE final_dataset_part1;

INSERT INTO final_dataset_part1
SELECT * FROM temp_final_dataset_part1;

DROP TABLE temp_final_dataset_part1;


SELECT DISTINCT *
INTO temp_final_dataset_part2
FROM final_dataset_part2;

TRUNCATE TABLE final_dataset_part2;

INSERT INTO final_dataset_part2
SELECT * FROM temp_final_dataset_part2;

DROP TABLE temp_final_dataset_part2;


SELECT DISTINCT *
INTO temp_final_dataset_part3
FROM final_dataset_part3;

TRUNCATE TABLE final_dataset_part3;

INSERT INTO final_dataset_part3
SELECT * FROM temp_final_dataset_part3;

DROP TABLE temp_final_dataset_part3;


SELECT DISTINCT *
INTO temp_final_dataset_part4
FROM final_dataset_part4;

TRUNCATE TABLE final_dataset_part4;

INSERT INTO final_dataset_part4
SELECT * FROM temp_final_dataset_part4;

DROP TABLE temp_final_dataset_part4;


SELECT DISTINCT *
INTO temp_final_dataset_part5
FROM final_dataset_part5;

TRUNCATE TABLE final_dataset_part5;

INSERT INTO final_dataset_part5
SELECT * FROM temp_final_dataset_part5;

DROP TABLE temp_final_dataset_part5;


SELECT DISTINCT *
INTO temp_final_dataset_part6
FROM final_dataset_part6;

TRUNCATE TABLE final_dataset_part6;

INSERT INTO final_dataset_part6
SELECT * FROM temp_final_dataset_part6;

DROP TABLE temp_final_dataset_part6;


SELECT DISTINCT *
INTO temp_final_dataset_part7
FROM final_dataset_part7;

TRUNCATE TABLE final_dataset_part7;

INSERT INTO final_dataset_part7
SELECT * FROM temp_final_dataset_part7;

DROP TABLE temp_final_dataset_part7;

INSERT INTO events_masters_final (
    event_id,
    event_type,
    summary,
    category,
    event_date,
    confidence,
    company1_id,
    company2_id,
    company1_name,
    company1_domain,
    company1_ticker,
    company2_name,
    company2_domain,
    company2_ticker
)
SELECT DISTINCT *
FROM final_dataset_part1 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);

INSERT INTO events_masters_final
SELECT DISTINCT *
FROM final_dataset_part2 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);

INSERT INTO events_masters_final
SELECT DISTINCT *
FROM final_dataset_part3 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);

INSERT INTO events_masters_final
SELECT DISTINCT *
FROM final_dataset_part4 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);


INSERT INTO events_masters_final
SELECT DISTINCT *
FROM final_dataset_part5 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);


INSERT INTO events_masters_final
SELECT DISTINCT *
FROM final_dataset_part6 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);


INSERT INTO events_masters_final
SELECT DISTINCT *
FROM final_dataset_part7 f
WHERE NOT EXISTS (
    SELECT 1 FROM events_masters_final m
    WHERE m.event_id = f.event_id
);

SELECT COUNT(*) AS total_rows FROM events_masters_final;
SELECT event_id, COUNT(*) AS cnt
FROM events_masters_final
GROUP BY event_id
HAVING COUNT(*) > 1;


ALTER TABLE events_masters_final
ALTER COLUMN event_date DATETIME;


CREATE INDEX idx_event_date ON events_masters_final(event_date);



IF EXISTS (SELECT name 
           FROM sys.indexes 
           WHERE name = 'idx_event_date' AND object_id = OBJECT_ID('events_masters_final'))
BEGIN
    DROP INDEX idx_event_date ON events_masters_final;
END


CREATE INDEX idx_event_date ON events_masters_final(event_date);

SELECT COUNT(*) AS total_rows
FROM events_masters_final;


SELECT TOP 10 *
FROM events_masters_final;

SELECT event_id, COUNT(*) AS cnt
FROM events_masters_final
GROUP BY event_id
HAVING COUNT(*) > 1;

ALTER TABLE events_masters_final
ALTER COLUMN event_type NVARCHAR(255);

ALTER TABLE events_masters_final
ALTER COLUMN category NVARCHAR(255);

ALTER TABLE events_masters_final
ALTER COLUMN company1_id NVARCHAR(100);

ALTER TABLE events_masters_final
ALTER COLUMN company2_id NVARCHAR(100);

ALTER TABLE events_masters_final
ALTER COLUMN company1_name NVARCHAR(255);

ALTER TABLE events_masters_final
ALTER COLUMN company2_name NVARCHAR(255);


CREATE INDEX idx_event_type ON events_masters_final(event_type);
CREATE INDEX idx_category ON events_masters_final(category);
CREATE INDEX idx_company1_id ON events_masters_final(company1_id);
CREATE INDEX idx_company2_id ON events_masters_final(company2_id);

WITH CombinedData AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_date DESC) AS rn
    FROM (
        SELECT * FROM dbo.final_dataset_part1
        UNION ALL
        SELECT * FROM dbo.final_dataset_part2
        UNION ALL
        SELECT * FROM dbo.final_dataset_part3
        UNION ALL
        SELECT * FROM dbo.final_dataset_part4
        UNION ALL
        SELECT * FROM dbo.final_dataset_part5
        UNION ALL
        SELECT * FROM dbo.final_dataset_part6
        UNION ALL
        SELECT * FROM dbo.final_dataset_part7
    ) AS AllParts
)
INSERT INTO dbo.events_masters_final (
    event_id, event_type, summary, category, event_date, confidence,
    company1_id, company2_id, company1_name, company1_domain, company1_ticker,
    company2_name, company2_domain, company2_ticker
)
SELECT 
    event_id, event_type, summary, category, event_date, confidence,
    company1_id, company2_id, company1_name, company1_domain, company1_ticker,
    company2_name, company2_domain, company2_ticker
FROM CombinedData
WHERE rn = 1
AND event_id NOT IN (SELECT event_id FROM dbo.events_masters_final);



IF OBJECT_ID('dbo.events_masters_final', 'U') IS NOT NULL
    DROP TABLE dbo.events_masters_final;
GO


CREATE TABLE dbo.events_masters_final (
    event_id UNIQUEIDENTIFIER PRIMARY KEY,
    event_type NVARCHAR(255),
    summary NVARCHAR(MAX),
    category NVARCHAR(100),
    event_date DATETIME,
    confidence DECIMAL(5,4),
    company1_id NVARCHAR(255),
    company2_id NVARCHAR(255),
    company1_name NVARCHAR(255),
    company1_domain NVARCHAR(255),
    company1_ticker NVARCHAR(50),
    company2_name NVARCHAR(255),
    company2_domain NVARCHAR(255),
    company2_ticker NVARCHAR(50)
);
GO


WITH Combined AS (
    SELECT * FROM dbo.final_dataset_part1
    UNION ALL
    SELECT * FROM dbo.final_dataset_part2
    UNION ALL
    SELECT * FROM dbo.final_dataset_part3
    UNION ALL
    SELECT * FROM dbo.final_dataset_part4
    UNION ALL
    SELECT * FROM dbo.final_dataset_part5
    UNION ALL
    SELECT * FROM dbo.final_dataset_part6
    UNION ALL
    SELECT * FROM dbo.final_dataset_part7
),
Deduplicated AS (
    SELECT *
    FROM Combined
    WHERE event_id IS NOT NULL
)

INSERT INTO dbo.events_masters_final
SELECT *
FROM Deduplicated AS D
WHERE D.event_id NOT IN (SELECT event_id FROM dbo.events_masters_final);
GO


CREATE NONCLUSTERED INDEX idx_event_date
ON dbo.events_masters_final(event_date);

CREATE NONCLUSTERED INDEX idx_event_type
ON dbo.events_masters_final(event_type);

CREATE NONCLUSTERED INDEX idx_company1_id
ON dbo.events_masters_final(company1_id);
GO



USE sample_news_events_db;
GO


IF OBJECT_ID('dbo.events_masters_final', 'U') IS NOT NULL
    DROP TABLE dbo.events_masters_final;
GO


CREATE TABLE dbo.events_masters_final (
    event_id UNIQUEIDENTIFIER PRIMARY KEY,
    event_type NVARCHAR(255),
    summary NVARCHAR(MAX),
    category NVARCHAR(255),
    event_date DATETIME,
    confidence DECIMAL(5,4),
    company1_id NVARCHAR(255),
    company2_id NVARCHAR(255),
    company1_name NVARCHAR(255),
    company1_domain NVARCHAR(255),
    company1_ticker NVARCHAR(50),
    company2_name NVARCHAR(255),
    company2_domain NVARCHAR(255),
    company2_ticker NVARCHAR(50)
);
GO


WITH CombinedData AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_date DESC) AS rn
    FROM (
        SELECT * FROM dbo.final_dataset_part1
        UNION ALL
        SELECT * FROM dbo.final_dataset_part2
        UNION ALL
        SELECT * FROM dbo.final_dataset_part3
        UNION ALL
        SELECT * FROM dbo.final_dataset_part4
        UNION ALL
        SELECT * FROM dbo.final_dataset_part5
        UNION ALL
        SELECT * FROM dbo.final_dataset_part6
        UNION ALL
        SELECT * FROM dbo.final_dataset_part7
    ) AS AllParts
)
INSERT INTO dbo.events_masters_final (
    event_id, event_type, summary, category, event_date, confidence,
    company1_id, company2_id, company1_name, company1_domain, company1_ticker,
    company2_name, company2_domain, company2_ticker
)
SELECT 
    event_id, event_type, summary, category, CAST(event_date AS DATETIME), confidence,
    company1_id, company2_id, company1_name, company1_domain, company1_ticker,
    company2_name, company2_domain, company2_ticker
FROM CombinedData
WHERE rn = 1;
GO


CREATE NONCLUSTERED INDEX idx_event_date ON dbo.events_masters_final(event_date);
CREATE NONCLUSTERED INDEX idx_event_type ON dbo.events_masters_final(event_type);
CREATE NONCLUSTERED INDEX idx_category ON dbo.events_masters_final(category);
CREATE NONCLUSTERED INDEX idx_company1_id ON dbo.events_masters_final(company1_id);
CREATE NONCLUSTERED INDEX idx_company2_id ON dbo.events_masters_final(company2_id);
GO


SELECT COUNT(*) AS total_rows FROM dbo.events_masters_final;
GO


SELECT TOP 10 * FROM dbo.events_masters_final;
GO


SELECT event_id, COUNT(*) AS cnt
FROM dbo.events_masters_final
GROUP BY event_id
HAVING COUNT(*) > 1;
GO

SELECT *
FROM dbo.events_masters_final;
EXEC sp_help 'dbo.events_masters_final';


