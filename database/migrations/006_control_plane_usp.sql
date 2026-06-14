-- -----------------------------------------------------------------------------
-- 006_control_plane_usp.sql
--
-- Legacy placeholder.
--
-- This migration used to be a monolithic control-plane USP file. The active
-- contract now lives in focused migrations:
--   005_ctl_core_usps.sql
--   007_ctl_onboarding_usps.sql
--   008_ctl_watermark_usps.sql
--   009_ctl_quality_usps.sql
--   019_ctl_lineage_usps.sql
--
-- Keep this file as a no-op so existing runbooks that execute files in numeric
-- order do not fail on a missing 006 step, while avoiding duplicate or
-- incompatible CREATE OR REPLACE definitions.
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS ctl;
