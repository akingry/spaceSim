# Gaia Sky binary extract

This folder is intentionally separate from the main database.

## Purpose

- Keep extracted Gaia Sky catalog data in immutable chunked files.
- Avoid writing directly into the live database during parsing.
- Preserve cross-reference identifiers for later database import.

## Layout

- `scripts/extract_gaiasky_bin.py` - parser/exporter for Gaia Sky particle binary version 3.
- `data/<bin-stem>/manifest.json` - extraction summary.
- `data/<bin-stem>/sample_records.json` - small human-readable sample.
- `data/<bin-stem>/stars/part-xxxxx.parquet` - main star records.
- `data/<bin-stem>/aliases/part-xxxxx.parquet` - alias/cross-reference records.

## Main star fields

- `source_id`
- `source_id_kind` (`gaia_dr3_like`, `gaia_named`, `hip_like`, or `ambiguous_small_id`)
- `gaia_source_id`
- `gaia_name` (only populated when the ID appears to be a Gaia identifier)
- `hip_id`
- `hip_name`
- `common_name`
- `raw_name`
- `x`, `y`, `z` (original Gaia Sky internal-unit coordinates)
- `distance_iu`, `distance_pc`
- `ra_rad`, `dec_rad`, `ra_deg`, `dec_deg`
- `x_eq_pc`, `y_eq_pc`, `z_eq_pc` (standard equatorial Cartesian coordinates in parsecs)
- `vx`, `vy`, `vz`
- `mu_alpha_mas_per_year`, `mu_delta_mas_per_year`
- `radial_velocity_km_per_s`
- `apparent_magnitude`, `absolute_magnitude`
- `color_packed_float`, `color_bits_hex`
- `size`
- `effective_temperature_k`
- `temperature_k`, `color_r`, `color_g`, `color_b`
- `has_valid_3d`, `excluded_from_3d`, `exclusion_reason`

## Alias table

The alias table stores all names found in the source plus synthesized Gaia DR3 names for easy cross-reference.
