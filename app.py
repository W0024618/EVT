-- Deduped detail records for IN.HYD on 2025-09-01
WITH CombinedQuery AS (
  SELECT 
    LocaleMessageTime = DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC),
    DateOnly         = CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)),
    Swipe_Time       = CONVERT(time(0), DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)),
    t1.ObjectName1,
    t1.ObjectName2 AS Door,
    Location         = t1.PartitionName2,
    CardNumber       = t5_card.CardNumber,
    AdmitCode        = t5_admit.value,
    Direction        = t5_dir.value,
    Rejection_Type   = t5_Rej.value,
    -- dedupe key: prefer Personnel.GUID, fallback to ObjectIdentity1
    PersonnelGUID    = COALESCE(t2.GUID, t1.ObjectIdentity1),
    t1.MessageType,
    t1.XmlGUID,
    t1.ObjectIdentity1,

    -- normalize PersonnelType: replace 'None' or NULL with 'Property Management'
    -- use CROSS/OUTER APPLY below to reference the normalized value in EmployeeID logic
    t3.Name AS PersonnelTypeRaw
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
    AND (t3.Name IN (
      'Employee',
      'Visitor',
      'Property Management',
      'Employee HYD',
      'Contractor HYD',
      'Visitor HYD',
      'Property Management HYD',
      'Temp Badge HYD',
      'None',
      'Terminated Personnel'
    ) OR t3.Name IS NULL)  -- include NULLs if present
),

-- normalize PersonnelType and compute EmployeeID based on the normalized type
Normalized AS (
  SELECT
    cq.*,
    PersonnelType = CASE 
                      WHEN cq.PersonnelTypeRaw IS NULL OR LTRIM(RTRIM(cq.PersonnelTypeRaw)) = 'None' 
                        THEN 'Property Management' 
                      ELSE cq.PersonnelTypeRaw 
                    END,
    -- EmployeeID choice follows the normalized PersonnelType
    EmployeeID = CASE
                   WHEN (CASE WHEN cq.PersonnelTypeRaw IS NULL OR LTRIM(RTRIM(cq.PersonnelTypeRaw)) = 'None' 
                               THEN 'Property Management' ELSE cq.PersonnelTypeRaw END)
                        IN ('Contractor','Terminated Contractor','Contractor HYD','Temp Badge HYD','Visitor HYD')
                     THEN cq_empl.Text12
                   ELSE CAST(cq_empl.Int1 AS NVARCHAR(50))
                 END
  FROM CombinedQuery cq
  LEFT JOIN [ACVSCore].[Access].[Personnel] cq_empl
    ON cq.ObjectIdentity1 = cq_empl.GUID
),

-- Rank per PersonnelGUID and keep earliest swipe (rn = 1)
Ranked AS (
  SELECT
    n.*,
    rn = ROW_NUMBER() OVER (PARTITION BY n.PersonnelGUID ORDER BY n.LocaleMessageTime ASC)
  FROM Normalized n
)

-- Final: details only (one row per PersonnelGUID), ordered by swipe time
SELECT
  LocaleMessageTime,
  DateOnly,
  Swipe_Time,
  PersonnelGUID,
  EmployeeID,
  PersonnelType,
  ObjectName1,
  Door,
  Location,
  CardNumber,
  AdmitCode,
  Direction,
  Rejection_Type,
  MessageType,
  XmlGUID,
  ObjectIdentity1
FROM Ranked
WHERE rn = 1
ORDER BY LocaleMessageTime ASC;


			  

			  



			  









Check below Query carefully and Update Query like 
Where Personnel Type is None replace as Property Management

-- Deduped detail records for IN.HYD on 2025-09-01 (earliest swipe per personnel GUID)
WITH CombinedQuery AS (
  SELECT 
    LocaleMessageTime = DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC),
    DateOnly = CONVERT(date, DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)),
    Swipe_Time = CONVERT(time(0), DATEADD(MINUTE, -1 * t1.MessageLocaleOffset, t1.MessageUTC)),
    t1.ObjectName1,
    t1.ObjectName2 AS Door,
    Location = t1.PartitionName2,
    CardNumber = t5_card.CardNumber,
    AdmitCode = t5_admit.value,
    Direction = t5_dir.value,
    Rejection_Type = t5_Rej.value,
    -- dedupe key: prefer Personnel.GUID, fallback to ObjectIdentity1
    PersonnelGUID = COALESCE(t2.GUID, t1.ObjectIdentity1),
    EmployeeID = CASE 
        WHEN t3.Name IN ('Contractor','Terminated Contractor','None','Contractor HYD','Temp Badge HYD','Visitor HYD') 
          THEN t2.Text12
        ELSE CAST(t2.Int1 AS NVARCHAR(50))
      END,
    PersonnelType = t3.Name,
    t1.MessageType,
    t1.XmlGUID,
	t1.ObjectIdentity1
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
	  'None',
      'Terminated Personnel'
    )
),

Ranked AS (
  -- Partition by PersonnelGUID so each GUID yields one row (earliest swipe)
  SELECT
    cq.*,
    rn = ROW_NUMBER() OVER (PARTITION BY cq.PersonnelGUID ORDER BY cq.LocaleMessageTime ASC)
  FROM CombinedQuery cq
)

-- Final: details only (one row per PersonnelGUID), ordered by swipe time
SELECT
  DateOnly,
  Swipe_Time,
  EmployeeID,
  CardNumber,
  PersonnelType,
  ObjectName1,
  Door,
  Location

FROM Ranked
WHERE rn = 1
ORDER BY LocaleMessageTime ASC;

