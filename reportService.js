//C:\Users\W0024618\Desktop\global-page\backend\services\reportService.js

 import { getPool, sql } from '../config/dbConfig.js';

export async function rawReport(region, { startDate, endDate, location, admitFilter = 'all' }) {
  if (!region) throw new Error('region required');
  if (!startDate || !endDate) throw new Error('startDate and endDate required');

  const pool = await getPool(region);
  const req = pool.request();

  // Accept NULL for location to mean "all partitions in this region DB"
  const locationParam = (location && String(location).trim()) ? String(location).trim() : null;

  req.input('location', sql.NVarChar(200), locationParam);
  req.input('startDate', sql.Date, startDate);
  req.input('endDate', sql.Date, endDate);
  req.input('admitFilter', sql.NVarChar(20), String(admitFilter || 'all'));

  const query = `
WITH CombinedQuery AS(
  SELECT 
     DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC) AS LocaleMessageTime,
     t1.ObjectName1,
     t1.PartitionName2       AS location,
     t5_card.CardNumber,
     t5_admit.value          AS AdmitCode,
     t5_dir.value            AS Direction,
     t1.ObjectName2          AS Door,
     t5_rej.value            AS Rejection_Type,
     CASE WHEN t3.Name IN ('Contractor','Terminated Contractor')
          THEN t2.Text12
          ELSE CAST(t2.Int1 AS NVARCHAR)
     END                       AS EmployeeID,
     t3.Name                  AS PersonnelType,
     t1.MessageType,
     t1.XmlGUID
  FROM ACVSUJournalLog AS t1
  LEFT JOIN ACVSCore.Access.Personnel     AS t2 ON t1.ObjectIdentity1 = t2.GUID
  LEFT JOIN ACVSCore.Access.PersonnelType AS t3 ON t2.PersonnelTypeId   = t3.ObjectID
  LEFT JOIN ACVSUJournalLogxmlShred       AS t5_admit
    ON t1.XmlGUID = t5_admit.GUID AND t5_admit.Name = 'AdmitCode'
  LEFT JOIN ACVSUJournalLogxmlShred       AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN ACVSUJournalLogxml             AS t_xml ON t1.XmlGUID = t_xml.GUID
  LEFT JOIN (
    SELECT GUID, [value]
    FROM ACVSUJournalLogxmlShred
    WHERE [Name] IN ('Card','CHUID')
  ) AS SCard ON t1.XmlGUID = SCard.GUID
  OUTER APPLY (
    SELECT COALESCE(
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID/Card)[1]', 'varchar(50)'),
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID)[1]', 'varchar(50)'),
      SCard.[value]
    ) AS CardNumber
  ) AS t5_card
  LEFT JOIN ACVSUJournalLogxmlShred AS t5_rej
    ON t1.XmlGUID = t5_rej.GUID AND t5_rej.Name = 'RejectCode'
  WHERE
    -- message types we consider
    t1.MessageType IN ('CardAdmitted' , 'CardRejected')
    -- location optional filter
    AND (@location IS NULL OR t1.PartitionName2 = @location)
    -- admit/reject filter: 'all' | 'admit' | 'reject' (case-insensitive)
    AND (
      UPPER(ISNULL(@admitFilter,'all')) = 'ALL'
      OR (UPPER(@admitFilter) = 'ADMIT'  AND t1.MessageType = 'CardAdmitted')
      OR (UPPER(@admitFilter) = 'REJECT' AND t1.MessageType = 'CardRejected')
    )
    AND CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) BETWEEN @startDate AND @endDate
)
SELECT
  LocaleMessageTime,
  CONVERT(date,    LocaleMessageTime) AS DateOnly,
  CONVERT(time(0), LocaleMessageTime) AS Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type
FROM CombinedQuery
ORDER BY LocaleMessageTime ASC;
`;

  const { recordset } = await req.query(query);
  return recordset;
}





export async function rejectionReport(region, { startDate, endDate, location } = {}) {
  if (!region) throw new Error('region required');
  if (!startDate || !endDate) throw new Error('startDate and endDate required');

  const pool = await getPool(region);
  const req = pool.request();

  const locationParam = (location && String(location).trim()) ? String(location).trim() : null;

  req.input('location', sql.NVarChar(200), locationParam);
  req.input('startDate', sql.Date, startDate);
  req.input('endDate', sql.Date, endDate);

  const query = `
WITH CombinedQuery AS(
  SELECT 
     DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC) AS LocaleMessageTime,
     t1.ObjectName1,
     t1.PartitionName2       AS location,
     t5_card.CardNumber,
     t5_admit.value          AS AdmitCode,
     t5_dir.value            AS Direction,
     t1.ObjectName2          AS Door,
     t5_rej.value            AS Rejection_Type,
     CASE WHEN t3.Name IN ('Contractor','Terminated Contractor')
          THEN t2.Text12
          ELSE CAST(t2.Int1 AS NVARCHAR)
     END                       AS EmployeeID,
     t3.Name                  AS PersonnelType,
     t1.MessageType,
     t1.XmlGUID
  FROM ACVSUJournalLog AS t1
  LEFT JOIN ACVSCore.Access.Personnel     AS t2 ON t1.ObjectIdentity1 = t2.GUID
  LEFT JOIN ACVSCore.Access.PersonnelType AS t3 ON t2.PersonnelTypeId   = t3.ObjectID
  LEFT JOIN ACVSUJournalLogxmlShred       AS t5_admit
    ON t1.XmlGUID = t5_admit.GUID AND t5_admit.Name = 'AdmitCode'
  LEFT JOIN ACVSUJournalLogxmlShred       AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN ACVSUJournalLogxml             AS t_xml ON t1.XmlGUID = t_xml.GUID
  LEFT JOIN (
    SELECT GUID, [value]
    FROM ACVSUJournalLogxmlShred
    WHERE [Name] IN ('Card','CHUID')
  ) AS SCard ON t1.XmlGUID = SCard.GUID
  OUTER APPLY (
    SELECT COALESCE(
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID/Card)[1]', 'varchar(50)'),
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID)[1]', 'varchar(50)'),
      SCard.[value]
    ) AS CardNumber
  ) AS t5_card
  LEFT JOIN ACVSUJournalLogxmlShred AS t5_rej
    ON t1.XmlGUID = t5_rej.GUID AND t5_rej.Name = 'RejectCode'
  WHERE
    t1.MessageType = 'CardRejected'
    AND (@location IS NULL OR t1.PartitionName2 = @location)
    AND CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) BETWEEN @startDate AND @endDate
),
Unified AS (
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'Lost'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'Clearance'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type IN ('CardDisabled','Disabled')
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'Stolen'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'Expired'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'PIN'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'UnknownCard'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'SiteCode'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'NotActivated'
  UNION ALL
  SELECT * FROM CombinedQuery WHERE Rejection_Type = 'FacilityCode'
)
SELECT
  LocaleMessageTime,
  CONVERT(date,    LocaleMessageTime) AS DateOnly,
  CONVERT(time(0), LocaleMessageTime) AS Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type
FROM Unified
ORDER BY LocaleMessageTime DESC;
`;

  const { recordset } = await req.query(query);
  return recordset;
}





export async function dailyAccessReportEMEA({ from, to }) {
  const pool = await getPool('emea');
  const req  = pool.request();

  req.input('fromDate', sql.Date, from);
  req.input('toDate',   sql.Date, to);

  const query = `
WITH SwipeData AS (
  SELECT
    t1.ObjectName1,
    t1.ObjectName2,
    t1.Messagetype,
    t2.Text12       AS EmployeeID,
    t3.Name         AS PersonnelType,
    t1.PartitionName2 AS location,

    /* true LOCAL time = UTC + offset */
    CAST(
      DATEADD(
        MINUTE,
        t1.MessageLocaleOffset,
        t1.MessageUTC
      ) AS datetime2(3)
    ) AS LocaleMessageTime,

    CASE
      WHEN t5_dir.Value = 'InDirection'  THEN 'IN'
      WHEN t5_dir.Value = 'OutDirection' THEN 'OUT'
      ELSE 'Unknown'
    END AS Swipe,

    t5_card.Value AS CardNumber
  FROM ACVSUJournal_00011027.dbo.ACVSUJournalLog AS t1
  INNER JOIN ACVSCore.Access.Personnel     AS t2
    ON t1.ObjectIdentity1 = t2.GUID
  INNER JOIN ACVSCore.Access.PersonnelType AS t3
    ON t2.PersonnelTypeId = t3.ObjectID
  LEFT JOIN ACVSUJournal_00011027.dbo.ACVSUJournalLogxmlShred AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID
    AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN ACVSUJournal_00011027.dbo.ACVSUJournalLogxmlShred AS t5_card
    ON t1.XmlGUID = t5_card.GUID
    AND t5_card.Value NOT IN ('InDirection','OutDirection')
    AND t5_card.Value NOT LIKE '%[^0-9]%'
    AND t5_card.Value IS NOT NULL
    AND LTRIM(RTRIM(t5_card.Value)) <> ''

  WHERE
    /* local‐time window:
         >= 08:00 on fromDate
         AND <  08:00 on the day after toDate */
    DATEADD(HOUR, 8, CAST(@fromDate AS DATETIME))
      <= DATEADD(MINUTE, t1.MessageLocaleOffset, t1.MessageUTC)
    AND DATEADD(MINUTE, t1.MessageLocaleOffset, t1.MessageUTC)
      < DATEADD(
          HOUR,
          8,
          DATEADD(DAY, 1, CAST(@toDate AS DATETIME))
        )

    AND t1.ObjectName1 IN (
      'Vainilaitis, Valdas',
      'Tomasevic, Kazimez',
      'Sesickis, Janas',
      'Valiunas, Sigitas'
    )
)
SELECT
  ObjectName1,
  ObjectName2,
  Messagetype,
  PersonnelType,
  EmployeeID,
  location,
  Swipe,
  CardNumber,
  LocaleMessageTime
FROM SwipeData
ORDER BY LocaleMessageTime;`;

  const { recordset } = await req.query(query);
  return recordset;
}

/**
 * In-vs-Out Report (parameterized) — groups by employee & month
 */
export async function inOutReport(region, { year, month, doors }) {
  const pool = await getPool(region);
  const req  = pool.request();

  req.input('TargetYear',  sql.Int, year);
  req.input('TargetMonth', sql.Int, month);
  doors.forEach((d, i) => req.input(`door${i}`, sql.NVarChar, d));
  const doorList = doors.map((_, i) => `@door${i}`).join(',');

  const query = `
WITH CombinedQuery AS (
  SELECT
    DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC) AS LocaleMessageTime,
    t1.ObjectName1,
    t1.PartitionName2    AS location,
    t5_card.CardNumber,
    t5_dir.value         AS Direction,
    CASE
      WHEN t3.Name IN ('Contractor','Terminated Contractor') THEN t2.Text12
      ELSE CAST(t2.Int1 AS NVARCHAR)
    END                    AS EmployeeID,
    t3.Name               AS PersonnelType
  FROM ACVSUJournalLog AS t1
  LEFT JOIN ACVSCore.Access.Personnel     AS t2
    ON t1.ObjectIdentity1 = t2.GUID
  LEFT JOIN ACVSCore.Access.PersonnelType AS t3
    ON t2.PersonnelTypeId = t3.ObjectID
  LEFT JOIN ACVSUJournalLogxmlShred AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID
    AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN ACVSUJournalLogxml AS t_xml
    ON t1.XmlGUID = t_xml.GUID
  LEFT JOIN (
    SELECT GUID, [value]
    FROM ACVSUJournalLogxmlShred
    WHERE [Name] IN ('Card','CHUID')
  ) AS SCard
    ON t1.XmlGUID = SCard.GUID
  OUTER APPLY (
    SELECT COALESCE(
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID/Card)[1]', 'varchar(50)'),
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID)[1]', 'varchar(50)'),
      SCard.[value]
    ) AS CardNumber
  ) AS t5_card
  WHERE
    YEAR(DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) = @TargetYear
    AND MONTH(DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) = @TargetMonth
    AND t1.ObjectName2 IN (${doorList})
)
SELECT
  FORMAT(LocaleMessageTime,'yyyy-MM')    AS Month,
  ObjectName1                           AS EmployeeName,
  EmployeeID,
  PersonnelType,
  location,
  SUM(CASE WHEN Direction='InDirection' THEN 1 ELSE 0 END)  AS In_Count,
  SUM(CASE WHEN Direction='OutDirection' THEN 1 ELSE 0 END) AS Out_Count,
  COUNT(*)                                                 AS TotalSwipes,
  SUM(CASE WHEN Direction='InDirection' THEN 1 ELSE 0 END)
  - SUM(CASE WHEN Direction='OutDirection' THEN 1 ELSE 0 END) AS InOut_Difference
FROM CombinedQuery
GROUP BY FORMAT(LocaleMessageTime,'yyyy-MM'),
         ObjectName1, EmployeeID, PersonnelType, location
ORDER BY Month DESC, EmployeeName;
`;

  const { recordset } = await req.query(query);
  return recordset;
}



/**
 * Time Duration Report (parameterized by region, partition, startDate)
 */
export async function timeDurationReport(region, { partition = 'Default', startDate }) {
  const pool = await getPool(region);
  const req  = pool.request();

  // bind inputs
  const fullPartition = `${region.toUpperCase()}.${partition}`;
  req.input('partition', sql.NVarChar, fullPartition);
  req.input('startDate', sql.Date, startDate);

  const query = `

SELECT 
    t1.[ObjectName1],
    t1.[ObjectName2],
    t1.[PartitionName2],
    CASE
        WHEN t2.[Int1] = 0 THEN t2.[Text12]
        ELSE CAST(t2.[Int1] AS NVARCHAR)
    END AS EmployeeID,
    t3.[Name] AS PersonnelType,
    t2.text5,
    DATEADD(MINUTE, -1 * t1.[MessageLocaleOffset], t1.[MessageUTC]) AS LocaleMessageTime,
    DATEADD(HOUR, -2, DATEADD(MINUTE, -1 * t1.[MessageLocaleOffset], t1.[MessageUTC])) AS AdjustedMessageTime
INTO 
    #CombinedEmployeeData
FROM 
    [ACVSUJournal_00010028].[dbo].[ACVSUJournalLog] AS t1
INNER JOIN 
    [ACVSCore].[Access].[Personnel] AS t2
    ON t1.ObjectIdentity1 = t2.GUID
INNER JOIN 
    [ACVSCore].[Access].[PersonnelType] AS t3
    ON t2.[PersonnelTypeId] = t3.[ObjectID];

-- Step 2: Daily duration per employee
WITH DailyDurations AS (
    SELECT 
        [ObjectName1],
        PersonnelType,
        EmployeeID,
        [PartitionName2],
        text5,
        CONVERT(DATE, AdjustedMessageTime) AS ShiftedDate,
        DATEPART(WEEK, AdjustedMessageTime) AS WeekNumber,
        DATEPART(YEAR, AdjustedMessageTime) AS YearNumber,
        MIN(LocaleMessageTime) AS FirstSwipeTime,
        MAX(LocaleMessageTime) AS LastSwipeTime,
        DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) AS DurationMinutes,
        RIGHT('00' + CAST(DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) / 60 AS NVARCHAR), 2)
        + ':' +
        RIGHT('00' + CAST(DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) % 60 AS NVARCHAR), 2) AS DurationHHMM,
        CASE 
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 5 THEN '<5mins'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 10 THEN '<10mins'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 20 THEN '<20mins'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 30 THEN '<30mins'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 60 THEN '<1hr'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 120 THEN 'Less than <2hrs'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 180 THEN 'Less than <3hrs'
            WHEN DATEDIFF(MINUTE, MIN(LocaleMessageTime), MAX(LocaleMessageTime)) < 240 THEN 'Less than <4hrs'
            ELSE '4+ hrs'
        END AS TimeDiffCategory
    FROM 
        #CombinedEmployeeData



  WHERE CONVERT(DATE, AdjustedMessageTime) >= @startDate

   AND [PartitionName2] = 'APAC.Default'
        AND PersonnelType IN ('Employee', 'Terminated Personnel')
    GROUP BY 
        [ObjectName1], EmployeeID, PersonnelType, text5, [PartitionName2],
        CONVERT(DATE, AdjustedMessageTime),
        DATEPART(WEEK, AdjustedMessageTime),
        DATEPART(YEAR, AdjustedMessageTime)
),

-- Step 3: Weekly summary
WeeklySummary AS (
    SELECT 
        EmployeeID,
        [ObjectName1],
        PersonnelType,
        text5,
        [PartitionName2],
        YearNumber,
        WeekNumber,
        COUNT(DISTINCT ShiftedDate) AS DaysPresentInWeek,
        SUM(CASE WHEN DurationMinutes < 240 THEN 1 ELSE 0 END) AS ViolationDaysInWeek
    FROM 
        DailyDurations
    GROUP BY 
        EmployeeID, [ObjectName1], PersonnelType, text5, [PartitionName2], YearNumber, WeekNumber
)

-- Step 4: Final output with daily duration, category, and defaulter flag
SELECT 
    dd.EmployeeID,
    dd.ObjectName1,
    dd.PersonnelType,
    dd.text5,
    dd.PartitionName2,
    dd.YearNumber,
    dd.WeekNumber,
    dd.ShiftedDate,
    dd.FirstSwipeTime,
    dd.LastSwipeTime,
    dd.DurationHHMM,
    dd.TimeDiffCategory,
    ws.DaysPresentInWeek,
    ws.ViolationDaysInWeek,
    (ws.DaysPresentInWeek - ws.ViolationDaysInWeek) AS CleanDaysInWeek,
    CASE
        WHEN (ws.DaysPresentInWeek - ws.ViolationDaysInWeek) < 3 THEN 'Yes'
        ELSE 'No'
    END AS Defaulter
FROM 
    DailyDurations dd
JOIN 
    WeeklySummary ws
    ON dd.EmployeeID = ws.EmployeeID
    AND dd.WeekNumber = ws.WeekNumber
    AND dd.YearNumber = ws.YearNumber
ORDER BY 
    dd.YearNumber DESC, dd.WeekNumber DESC, dd.EmployeeID, dd.ShiftedDate;
  `;

  const { recordset } = await req.query(query);
  return recordset;
}


//EUROC 




export async function eurocAdmitRejectionReport(region, { reportDate }) {
  const pool = await getPool(region);
  const req  = pool.request();

  // enforce location LT.Vilnius (as requested)
  req.input('location', sql.NVarChar, 'LT.Vilnius');
  // reportDate should be YYYY-MM-DD or Date — bind as sql.Date
  req.input('reportDate', sql.Date, reportDate);

  const query = `
/*
  Approach:
  1) Build CombinedQuery as a CTE and SELECT INTO #Combined (materialize)
  2) Build #Admits (with ROW_NUMBER) and #Rejections from #Combined
  3) Return three resultsets: admits (rn=1), rejections (all), summary (counts)
*/

WITH CombinedQuery AS(
  SELECT 
     DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC) AS LocaleMessageTime,
     t1.ObjectName1,
     t1.PartitionName2       AS location,
     t5_card.CardNumber,
     t5_admit.value          AS AdmitCode,
     t5_dir.value            AS Direction,
     t1.ObjectName2          AS Door,
     t5_rej.value            AS Rejection_Type,
     CASE WHEN t3.Name IN ('Contractor','Terminated Contractor')
          THEN t2.Text12
          ELSE CAST(t2.Int1 AS NVARCHAR)
     END                       AS EmployeeID,
     t3.Name                  AS PersonnelType,
     t1.MessageType,
     t1.XmlGUID
  FROM ACVSUJournalLog AS t1
  LEFT JOIN ACVSCore.Access.Personnel     AS t2 ON t1.ObjectIdentity1 = t2.GUID
  LEFT JOIN ACVSCore.Access.PersonnelType AS t3 ON t2.PersonnelTypeId   = t3.ObjectID
  LEFT JOIN ACVSUJournalLogxmlShred       AS t5_admit
    ON t1.XmlGUID = t5_admit.GUID AND t5_admit.Name = 'AdmitCode'
  LEFT JOIN ACVSUJournalLogxmlShred       AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN ACVSUJournalLogxml             AS t_xml ON t1.XmlGUID = t_xml.GUID
  LEFT JOIN (
    SELECT GUID, [value]
    FROM ACVSUJournalLogxmlShred
    WHERE [Name] IN ('Card','CHUID')
  ) AS SCard ON t1.XmlGUID = SCard.GUID
  OUTER APPLY (
    SELECT COALESCE(
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID/Card)[1]', 'varchar(50)'),
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID)[1]', 'varchar(50)'),
      SCard.[value]
    ) AS CardNumber
  ) AS t5_card
  LEFT JOIN ACVSUJournalLogxmlShred AS t5_rej
    ON t1.XmlGUID = t5_rej.GUID AND t5_rej.Name = 'RejectCode'
  WHERE
    t1.MessageType IN ('CardAdmitted' , 'CardRejected')
    AND t1.PartitionName2 = @location
    AND CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) = @reportDate
)

-- materialize CombinedQuery into a temp table
SELECT
  LocaleMessageTime,
  CONVERT(date, LocaleMessageTime) AS DateOnly,
  CONVERT(time(0), LocaleMessageTime) AS Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type,
  MessageType
INTO #Combined
FROM CombinedQuery;

-- create admits with row number (first admit per employee)
SELECT
  LocaleMessageTime,
  DateOnly,
  Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type,
  ROW_NUMBER() OVER (PARTITION BY ISNULL(EmployeeID, CardNumber) ORDER BY LocaleMessageTime ASC) AS rn
INTO #Admits
FROM #Combined
WHERE MessageType = 'CardAdmitted'
  AND PersonnelType = 'Employee';

-- create rejections (all)
SELECT
  LocaleMessageTime,
  DateOnly,
  Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type
INTO #Rejections
FROM #Combined
WHERE MessageType = 'CardRejected';

-- resultset 1: admits (only rn = 1)
SELECT
  LocaleMessageTime,
  DateOnly,
  Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type
FROM #Admits
WHERE rn = 1
ORDER BY LocaleMessageTime ASC;

-- resultset 2: rejections (all)
SELECT
  LocaleMessageTime,
  DateOnly,
  Swipe_Time,
  EmployeeID,
  ObjectName1,
  PersonnelType,
  location,
  CardNumber,
  AdmitCode,
  Direction,
  Door,
  Rejection_Type
FROM #Rejections
ORDER BY LocaleMessageTime ASC;

-- resultset 3: summary counts by Rejection_Type
SELECT
  ISNULL(Rejection_Type, 'Unknown') AS Rejection_Type,
  COUNT(*) AS CountVal
FROM #Rejections
GROUP BY ISNULL(Rejection_Type, 'Unknown')
ORDER BY CountVal DESC;

-- cleanup temp tables (optional — they scope to the session and will go away automatically,
-- but good practice to drop)
DROP TABLE IF EXISTS #Admits;
DROP TABLE IF EXISTS #Rejections;
DROP TABLE IF EXISTS #Combined;
`;

  const result = await req.query(query);
  // result.recordsets is an array of resultsets
  const recordsets = result.recordsets || [];
  const admitRows = (recordsets[0] || []).map(r => ({
    LocaleMessageTime: r.LocaleMessageTime,
    DateOnly: r.DateOnly,
    Swipe_Time: r.Swipe_Time,
    ObjectName1: r.ObjectName1,
    EmployeeID: r.EmployeeID,
    PersonnelType: r.PersonnelType,
    CardNumber: r.CardNumber,
    Door: r.Door,
    location: r.location,
    Direction: r.Direction
  }));
  const rejectRows = (recordsets[1] || []).map(r => ({
    LocaleMessageTime: r.LocaleMessageTime,
    DateOnly: r.DateOnly,
    Swipe_Time: r.Swipe_Time,
    ObjectName1: r.ObjectName1,
    EmployeeID: r.EmployeeID,
    PersonnelType: r.PersonnelType,
    CardNumber: r.CardNumber,
    Rejection_Type: r.Rejection_Type,
    Door: r.Door,
    location: r.location,
    Direction: r.Direction
  }));
  const summaryRows = (recordsets[2] || []).map(r => ({ Rejection_Type: r.Rejection_Type, Count: r.CountVal }));

  return { admit: admitRows, rejection: rejectRows, summary: summaryRows };
}











































