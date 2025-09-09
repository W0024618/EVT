-- 1) Pull raw rows and resolve CardNumber + Personnel GUID
WITH CombinedQuery AS (
  SELECT 
    LocaleMessageTime = DATEADD(MINUTE, -1 * t1.[MessageLocaleOffset], t1.[MessageUTC]),
    t1.ObjectName1,
    t1.PartitionName2 AS Location,
    CardNumber = t5_card.CardNumber,
    AdmitCode = t5_admit.value,
    Direction = t5_dir.value,
    t1.ObjectName2,
    Rejection_Type = t5_Rej.value,
    -- Prefer canonical personnel GUID if present, fallback to ObjectIdentity1
    PersonnelGUID = COALESCE(t2.GUID, t1.ObjectIdentity1),
    CASE 
      WHEN t3.Name IN ('Contractor','Terminated Contractor','Contractor HYD','Temp Badge HYD','Visitor HYD') 
        THEN t2.Text12
      ELSE CAST(t2.Int1 AS NVARCHAR(50))
    END AS EmployeeID,
    PersonnelType = t3.Name,
    t1.MessageType,
    t1.XmlGUID
  FROM [ACVSUJournal_00010029].[dbo].[ACVSUJournalLog] AS t1
  LEFT JOIN [ACVSCore].[Access].[Personnel]      AS t2 ON t1.ObjectIdentity1 = t2.GUID
  LEFT JOIN [ACVSCore].[Access].[PersonnelType]  AS t3 ON t2.PersonnelTypeId = t3.ObjectID
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_admit
    ON t1.XmlGUID = t5_admit.GUID AND t5_admit.Name = 'AdmitCode'
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID AND t5_dir.Value IN ('InDirection','OutDirection')
  LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxml] AS t_xml
    ON t1.XmlGUID = t_xml.GUID
  LEFT JOIN (
    SELECT GUID, [value]
    FROM [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred]
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
    AND t1.PartitionName2 = 'IN.HYD'           -- Hyderabad partition
    AND CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) = '2025-09-01'
    AND t3.Name IN (
      'Employee', 'Visitor', 'Property Management',
      'Employee HYD', 'Contractor HYD', 'Visitor HYD', 'Property Management HYD',
      'Temp Badge HYD', 'Terminated Personnel'
    )
),

-- 2) De-duplicate by PersonnelGUID: keep the earliest swipe per GUID for the date
Dedup AS (
  SELECT *
  FROM (
    SELECT
      dq.*,
      RN = ROW_NUMBER() OVER (PARTITION BY dq.PersonnelGUID ORDER BY dq.LocaleMessageTime ASC)
    FROM CombinedQuery dq
    WHERE dq.PersonnelGUID IS NOT NULL
  ) t
  WHERE RN = 1
),

-- 3) Counting per PersonnelType (from deduped set)
Counts AS (
  SELECT
    PersonnelType,
    HeadCount = COUNT(*) -- each row is a unique personnel (deduped)
  FROM Dedup
  GROUP BY PersonnelType
),

-- 4) Ensure all requested PersonnelTypes appear (show 0 if missing)
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

-- Final results: per-type counts + grand total
SELECT
  dt.PersonnelType,
  ISNULL(c.HeadCount, 0) AS HeadCount
FROM DesiredTypes dt
LEFT JOIN Counts c ON c.PersonnelType = dt.PersonnelType
ORDER BY dt.PersonnelType;

-- Total distinct headcount (single number)
SELECT COUNT(*) AS Total_Distinct_Headcount
FROM Dedup;











Check below Query and Update Query for Hydeabad ,and give me Hydrabad 01-09-2025 date HeadCount..

       add Personnel type , Employee, Visitor, Property Management, Employee HYD, Contractor HYD, Visitor HYD, Property Management HYD, Temp Badge HYD, Terminated Personnel  
Give me Headcount , remove Duplicate using personeel GUID
       


WITH CombinedQuery AS(
  SELECT 
     DATEADD(MINUTE, -1 * t1.[MessageLocaleOffset], t1.[MessageUTC]) AS LocaleMessageTime,
    t1.ObjectName1,
 t1.PartitionName2 As location,
 t5_card.CardNumber,
t5_admit.value AS AdmitCode,
t5_dir.value AS Direction,
    t1.ObjectName2,
 t5_rej.value AS Rejection_Type,
 CASE 
        WHEN t3.Name IN ('Contractor', 'Terminated Contractor' ,'Contractor HYD', 'Temp Badge HYD' ,'Visitor HYD') THEN t2.Text12
        ELSE CAST(t2.Int1 AS NVARCHAR)
    END AS "EmployeeID",
    t3.Name AS PersonnelType,
	t1.PartitionName2,
    t1.MessageType,t1.XmlGUID
 FROM
    [ACVSUJournal_00010029].[dbo].[ACVSUJournalLog] AS t1
LEFT JOIN
    [ACVSCore].[Access].[Personnel] AS t2
    ON t1.ObjectIdentity1 = t2.GUID
LEFT JOIN
    [ACVSCore].[Access].[PersonnelType] AS t3
    ON t2.[PersonnelTypeId] = t3.[ObjectID]
LEFT JOIN
    [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_admit
    ON t1.XmlGUID = t5_admit.GUID
    AND t5_admit.Name = 'AdmitCode'
LEFT JOIN
    [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_dir
    ON t1.XmlGUID = t5_dir.GUID
    AND t5_dir.Value IN ('InDirection', 'OutDirection')
    LEFT JOIN [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxml] AS t_xml
        ON t1.XmlGUID = t_xml.GUID
    -- Pre-pull shredded  Card  row
    LEFT JOIN (
    SELECT GUID, [value]
    FROM [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred]
    WHERE [Name] IN ('Card', 'CHUID')
    ) AS SCard
    ON t1.XmlGUID = SCard.GUID
    /* NEW: three-stage CardNumber resolution */
    OUTER APPLY (
    SELECT COALESCE(
        -- 1) <LogMessage><CHUID><Card> </Card></CHUID>
        TRY_CAST(t_xml.XmlMessage AS XML)
        .value('(/LogMessage/CHUID/Card)[1]', 'varchar(50)'),
        -- 2) <LogMessage><CHUID> </CHUID> (no nested <Card>)
        TRY_CAST(t_xml.XmlMessage AS XML)
        .value('(/LogMessage/CHUID)[1]', 'varchar(50)'),
        -- 3) shredded fallback
        SCard.[value]
    ) AS CardNumber
    ) AS t5_card
 
LEFT JOIN
    [ACVSUJournal_00010029].[dbo].[ACVSUJournalLogxmlShred] AS t5_Rej
    ON t1.XmlGUID = t5_Rej.GUID
    AND t5_Rej.Name = 'RejectCode'
 
   
   --include both admits and rejects
   WHERE t1.MessageType = 'CardAdmitted' AND t3.Name IN ('Employee', 'Terminated Personnel','Contractor HYD', 'Temp Badge HYD' ,'Visitor HYD')
   AND t1.PartitionName2 = 'IN.HYD'
   AND CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)) >= '2025-09-01')
 
SELECT 
 LocaleMessageTime,
 Dateonly = Convert(date, LocaleMessageTime), 
 Swipe_Time = CONVERT(time(0), LocaleMessageTime),
 EmployeeID,
 ObjectName1,
 PersonnelType,
 PartitionName2,
 CardNumber,
 AdmitCode,
 Direction,
 ObjectName2 As Door,
 Rejection_Type
 FROM CombinedQuery
 Order BY LocaleMessageTime ASC;
