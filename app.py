We Dont need Summary we need Details Record only...

(9 rows affected)
Msg 208, Level 16, State 1, Line 100
Invalid object name 'Dedup'.

Completion time: 2025-09-09T13:16:00.2672821+05:30




-- Hyderabad headcount for 2025-09-01, deduped by Personnel GUID
WITH CombinedQuery AS (
  SELECT 
    LocaleMessageTime = DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC),
    t1.ObjectName1,
    Location = t1.PartitionName2,
    CardNumber = t5_card.CardNumber,
    AdmitCode = t5_admit.value,
    Direction = t5_dir.value,
    Door = t1.ObjectName2,
    Rejection_Type = t5_Rej.value,
    PersonnelGUID = COALESCE(t2.GUID, t1.ObjectIdentity1), -- dedupe key
    EmployeeID = CASE 
        WHEN t3.Name IN ('Contractor','Terminated Contractor','Contractor HYD','Temp Badge HYD','Visitor HYD') 
          THEN t2.Text12
        ELSE CAST(t2.Int1 AS NVARCHAR(50))
      END,
    PersonnelType = t3.Name,
    t1.MessageType,
    t1.XmlGUID
  FROM [ACVSUJournal_00010029].[dbo].[ACVSUJournalLog] AS t1
  LEFT JOIN [ACVSCore].[Access].[Personnel]     AS t2 ON t1.ObjectIdentity1 = t2.GUID
  LEFT JOIN [ACVSCore].[Access].[PersonnelType] AS t3 ON t2.PersonnelTypeId   = t3.ObjectID
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_admit
    ON t1.XmlGUID = t5_admit.GUID AND t5_admit.Name = 'AdmitCode'
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxml] AS t_xml
    ON t1.XmlGUID = t_xml.GUID
  LEFT JOIN (
    SELECT GUID, [value] FROM [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred]
    WHERE [Name] IN ('Card','CHUID')
  ) AS SCard ON t1.XmlGUID = SCard.GUID
  OUTER APPLY (
    SELECT COALESCE(
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID/Card)[1]','varchar(50)'),
      TRY_CAST(t_xml.XmlMessage AS XML).value('(/LogMessage/CHUID)[1]','varchar(50)'),
      SCard.[value]
    ) AS CardNumber
  ) AS t5_card
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_Rej
    ON t1.XmlGUID = t5_Rej.GUID AND t5_Rej.Name = 'RejectCode'
  WHERE
    t1.MessageType = 'CardAdmitted'
    AND t1.PartitionName2 = 'IN.HYD'
    AND CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) = '2025-09-01'
    AND t3.Name IN (
      'Employee',
      'Visitor',
      'Property Management',
      'Employee HYD',
      'Contractor HYD',
      'Visitor HYD',
      'Property Management HYD',
      'Temp Badge HYD',
      'Terminated Personnel'
    )
),

-- keep only one row per PersonnelGUID (earliest swipe on that date)
Dedup AS (
  SELECT *
  FROM (
    SELECT
      dq.*,
      RN = ROW_NUMBER() OVER (PARTITION BY dq.PersonnelGUID ORDER BY dq.LocaleMessageTime ASC)
    FROM CombinedQuery dq
    WHERE dq.PersonnelGUID IS NOT NULL
  ) x
  WHERE RN = 1
),

Counts AS (
  SELECT PersonnelType, HeadCount = COUNT(*) 
  FROM Dedup
  GROUP BY PersonnelType
),

-- ensure all requested types appear (0 when absent)
DesiredTypes AS (
  SELECT 'Employee' AS PersonnelType UNION ALL
  SELECT 'Visitor' UNION ALL
  SELECT 'Property Management' UNION ALL
  SELECT 'Employee HYD' UNION ALL
  SELECT 'Contractor HYD' UNION ALL
  SELECT 'Visitor HYD' UNION ALL
  SELECT 'Property Management HYD' UNION ALL
  SELECT 'Temp Badge HYD' UNION ALL
  SELECT 'Terminated Personnel'
)

SELECT
  dt.PersonnelType,
  ISNULL(c.HeadCount,0) AS HeadCount
FROM DesiredTypes dt
LEFT JOIN Counts c ON c.PersonnelType = dt.PersonnelType
ORDER BY dt.PersonnelType;

-- single total distinct headcount for the date (deduped)
SELECT COUNT(*) AS Total_Distinct_Headcount FROM Dedup;

-- OPTIONAL: sample deduped list (GUID, EmployeeID, PersonnelType, first swipe time)
SELECT PersonnelGUID, EmployeeID, PersonnelType, LocaleMessageTime
FROM Dedup
ORDER BY LocaleMessageTime;





Refer below APAC HeadCount query 



WITH CombinedEmployeeData AS (
    SELECT
        t1.[ObjectName1],
        t1.[ObjectName2],
        CASE
            WHEN t2.[Int1] = 0 THEN t2.[Text12]
            ELSE CAST(t2.[Int1] AS NVARCHAR)
        END AS EmployeeID,
        t2.[PersonnelTypeID],
        t3.[Name] AS PersonnelTypeName,
		t1.Objectidentity1 AS EmployeeIdentity,
        CASE
            WHEN t1.ObjectName2 LIKE 'APAC_PI%' THEN 'Taguig City'
            WHEN t1.ObjectName2 LIKE 'APAC_PH%' THEN 'Quezon City'
            WHEN t1.ObjectName2 LIKE '%PUN%' THEN 'Pune'
            ELSE t1.PartitionName2
        END AS Location,
        t1.PartitionName2,
        DATEADD(MINUTE, -1 * t1.[MessageLocaleOffset], t1.[MessageUTC]) AS LocaleMessageTime,
        t1.MessageType
    FROM
        [ACVSUJournal_00010028].[dbo].[ACVSUJournalLog] AS t1
        INNER JOIN [ACVSCore].[Access].[Personnel] AS t2 ON t1.ObjectIdentity1 = t2.GUID
        INNER JOIN [ACVSCore].[Access].[PersonnelType] AS t3 ON t2.PersonnelTypeID = t3.ObjectID
    WHERE
        t1.MessageType = 'CardAdmitted'
        AND t1.PartitionName2 IN ('APAC.Default', 'CN.Beijing', 'JP.Tokyo', 'PH.Manila', 'MY.Kuala Lumpur')
        AND CONVERT(DATE, DATEADD(MINUTE, -1 * t1.[MessageLocaleOffset], t1.[MessageUTC])) = '2025-06-26'
),
RankedEmployeeData AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY EmployeeIdentity, CONVERT(DATE, LocaleMessageTime) ORDER BY LocaleMessageTime DESC) AS rn
    FROM
        CombinedEmployeeData
)
SELECT
    [ObjectName1],
    PersonnelTypeName,
    EmployeeID,
    Location,
    MessageType,
    CONVERT(DATE, LocaleMessageTime) AS Date
FROM
    RankedEmployeeData
WHERE
    rn = 1;

	--DROP Table #CombinedEmployeeData;
