USE [master];
GO

IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'weather')
BEGIN
    CREATE LOGIN [weather] WITH PASSWORD = 'Password123', CHECK_POLICY = OFF;
    ALTER SERVER ROLE [sysadmin] ADD MEMBER [weather];
END
GO
CREATE DATABASE WeatherDB;
GO
USE WeatherDB;
GO

CREATE TABLE WeatherData (
    id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100),
    weather VARCHAR(50),
    temperature FLOAT,
    temperature_feels_like FLOAT,
    wind_speed FLOAT,
    pressure INT,
    visibility INT,
    humidity INT,
    event_suitability VARCHAR(20),
    safety_warning VARCHAR(50),
    comfort_level VARCHAR(20),
    recorded_at DATETIME DEFAULT GETDATE()
);
GO