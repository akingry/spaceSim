# spaceSim

A local Python/OpenGL star viewer built from a merged **Hipparcos + Gaia** stellar dataset.

The project has two halves:
- a data pipeline that builds local SQLite catalogs from Hipparcos and a Gaia Sky / Gaia DR3-derived extract
- a fullscreen interactive viewer for flying through the resulting 3D star field

## Current status

The data pipeline is in good shape and the merged catalog is being generated successfully.

Current local merged dataset:
- **646,451** total stars
- **117,955** Hipparcos rows
- **646,400** Gaia extract rows
- **117,904** overlap matches
- **528,496** Gaia-only additions
- **51** Hipparcos-only rows
- **588,754** stars flagged as valid 3D

The viewer is mid-integration after recent radius/sphere-rendering work. The README documents the intended architecture and controls, but the current branch should be treated as **active development**, not a polished release.

## Highlights

- Fullscreen 3D starfield renderer using `pygame` + `moderngl`
- Hipparcos + Gaia merged SQLite database
- Naming priority that preserves useful Hipparcos/common names where possible
- Magnitude-based visibility filtering
- Mouse-look flight controls with adjustable speed
- Inspect mode with star info panel
- Goto mode for traveling to a selected star
- Derived stellar temperatures, colors, luminosities, and render-safe radii
- Separate Gaia extract staging area to avoid mutating the live DB during parsing

## Repository layout

```text
spaceSim/
├─ README.md
├─ run_star_viewer.cmd
├─ star_viewer.py
├─ build_hipparcos_db.py
├─ merge_hipparcos_supplements.py
├─ build_merged_hip_gaia_db.py
├─ add_radius_to_merged_db.py
├─ clamp_radius_in_merged_db.py
├─ import_iau_names.py
├─ add_common_names_from_simbad.py
├─ gaia_extract/
│  ├─ README.md
│  ├─ scripts/
│  └─ data/
├─ catalog/
└─ *.db / source catalog files
```

## Data pipeline

### 1) Hipparcos database

`build_hipparcos_db.py` builds `hipparcos.db` from the Hipparcos New Reduction catalog.

It:
- reads the source table
- keeps source fields available for inspection
- parses important numeric fields
- computes equatorial 3D positions from RA/Dec/parallax
- stores helper flags for 3D usability

Useful derived Hipparcos fields include:
- `ra_deg`, `dec_deg`
- `distance_pc`
- `pmra_masyr`, `pmdec_masyr`
- `hpmag_num`
- `temperature_k`
- `color_r`, `color_g`, `color_b`
- `x`, `y`, `z`
- `has_valid_3d`, `excluded_from_3d`, `exclusion_reason`
- `common_name`

### 2) Hipparcos supplement merge

`merge_hipparcos_supplements.py` adds supplemental tables and a joined `stars_expanded` view.

Included supplemental inputs:
- `hip7p.dat`
- `hip9p.dat`
- `hipvim.dat`

### 3) Gaia extract staging

The Gaia-side data currently comes from a Gaia Sky binary catalog extract (`gaia-dr3-best`).

Raw binary inputs live under:
- `catalog\gaia-dr3-best\metadata.bin`
- `catalog\gaia-dr3-best\particles\particles_000000.bin`

These are unpacked into parquet files under:
- `gaia_extract\data\particles_000000\stars\*.parquet`
- `gaia_extract\data\particles_000000\aliases\*.parquet`

This keeps extraction work separate from the live SQLite databases.

### 4) Merged database

`build_merged_hip_gaia_db.py` creates `hip_gaia_merged.db`.

Merge policy:
- keep Hipparcos-only stars
- include Gaia-only stars
- when both catalogs refer to the same star, prefer Hipparcos values
- fill missing compatible fields from Gaia where useful

Important merged fields include:
- `merge_key`
- `has_hip`, `has_gaia`
- `preferred_catalog`
- `hip_id_int`
- `gaia_source_id`
- `merged_display_name`
- `merged_common_name`
- `merged_apparent_magnitude`
- `merged_absolute_magnitude`
- `merged_radial_velocity_km_s`

### 5) Radius derivation and clamping

`add_radius_to_merged_db.py` derives:
- `luminosity_lsun`
- `radius_rsun`
- `radius_m`
- `radius_method`
- `radius_temp_source`
- `radius_mag_source`

`clamp_radius_in_merged_db.py` preserves the raw radius estimate and produces a safer render-ready radius:
- `radius_rsun_raw`
- `radius_m_raw`
- `radius_class`
- `radius_bounds_min_rsun`
- `radius_bounds_max_rsun`
- `radius_is_fallback`
- `radius_clamp_reason`

Recommended interpretation:
- `radius_rsun_raw` = original estimate
- `radius_rsun` = safer value for rendering

## Viewer

### Entry point

Run:

```bat
run_star_viewer.cmd
```

or directly:

```bash
python star_viewer.py
```

### Controls

- `W A S D` — move
- `E / Q` — move up/down
- mouse — look around
- mouse wheel — adjust travel speed
- `[` / `]` — decrease/increase visible magnitude limit
- right click — toggle inspect mode
- left click (inspect mode) — select a star
- `G` — begin goto mode for selected star
- `H` — toggle info panel
- `Home` — return to the home/origin position
- `Esc` — quit

### Intended viewer behavior

The viewer is designed to:
- render stars from the merged database
- size/opacity stars by apparent magnitude
- show a subtle galactic reference overlay while at the home position
- label nearby stars using merged naming priority
- show a star info panel in inspect mode
- render the goto target as a sphere during close approach

## Dependencies

Python packages used by the project:
- `numpy`
- `pygame`
- `moderngl`
- `pyrr`
- `astropy`
- `pyarrow`

A simple install example:

```bash
pip install -r requirements.txt
```

## Data sources

### Hipparcos

CDS / Hipparcos New Reduction:
- https://cdsarc.cds.unistra.fr/ftp/I/311/ReadMe
- https://cdsarc.cds.unistra.fr/ftp/I/311/hip2.dat
- https://cdsarc.cds.unistra.fr/ftp/I/311/hip7p.dat
- https://cdsarc.cds.unistra.fr/ftp/I/311/hip9p.dat
- https://cdsarc.cds.unistra.fr/ftp/I/311/hipvim.dat

### Gaia / Gaia Sky

Current dataset metadata is based on the Gaia Sky `gaia-dr3-best` catalog extract.

## Caveats

This is a visualization-first project, not a publication-grade astrophysics pipeline.

Practical compromises include:
- merge-preference rules between catalogs
- approximate temperature/color derivations
- radius estimation from magnitude + temperature
- class-based radius fallback/clamping for render stability
- apparent magnitude recomputation relative to the current observer position

## Roadmap

Likely next improvements:
- finish the current radius-aware sphere rendering integration
- improve multiple-star companion detection
- add a search UI for stars by name / identifier
- expose raw vs clamped radius in the info panel
- add screenshots or short demo media
- tighten repo hygiene around generated data artifacts

## Notes for contributors

Large generated data artifacts currently exist locally as part of development. The long-term repo shape should probably keep code and small metadata in Git while treating extracted catalogs and generated databases as rebuildable artifacts.
