SELECT flightStatusPublic, COUNT(*) AS total_vols, SUM(CASE WHEN flightLegs_irregularity_delayDuration > 0 THEN 1 ELSE 0 END) AS vols_avec_irregularite,
SUM(CASE WHEN flightLegs_irregularity_delayDuration > 0 THEN 1 ELSE 0 END) * 100/ COUNT(*) AS pourcentage_irregularites
FROM "@file:c:\Users\modo-\Desktop\Formation DataScientest\Projet\CSV\csv_exploration.csv"
GROUP BY flightStatusPublic
ORDER BY vols_avec_irregularite DESC;