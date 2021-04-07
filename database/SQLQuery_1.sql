-- DROP TABLE [trading-sql-db].[dbo].[TD_ML_Bot_Base_Fts]
-- DROP TABLE [trading-sql-db].[dbo].[TD_ML_Bot_Eng_Fts]

-- SELECT * FROM TD_ML_Bot_Base_Fts
SELECT * FROM TD_ML_Bot_Eng_Fts

/*
SELECT * 
FROM TD_ML_Bot_Base_Fts
WHERE date = (SELECT max(date) FROM TD_ML_Bot_Base_Fts)
*/