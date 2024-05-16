-- Databricks notebook source
-- TABLE:VIEW
-- ProspectIncremental:v_Prospect
-- FinWire:v_FinWire
-- BatchDate:v_BatchDate

INSERT INTO {tgt_db}.{table}
SELECT * FROM {wh_db}_{scale_factor}_stage.{view};
