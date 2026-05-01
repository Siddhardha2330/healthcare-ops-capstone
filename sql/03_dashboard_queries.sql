SELECT * FROM health_cat.gold.gold_no_show_rate ORDER BY no_show_rate_pct DESC;

SELECT * FROM health_cat.gold.gold_doctor_utilization ORDER BY total_appointments DESC;

SELECT * FROM health_cat.gold.gold_department_revenue ORDER BY total_revenue DESC;

SELECT * FROM health_cat.gold.gold_wait_time_trends ORDER BY visit_month, avg_wait_time_minutes DESC;

SELECT * FROM health_cat.gold.gold_patient_revisit_rate ORDER BY visit_count DESC;

SELECT * FROM health_cat.gold.gold_diagnostics_volume ORDER BY test_count DESC;

SELECT * FROM health_cat.gold.gold_feedback_summary;

SELECT * FROM health_cat.gold.gold_kaggle_no_show_by_age ORDER BY no_show_rate_pct DESC;

