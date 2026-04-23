# spaceSim

A local Earth-centered 3D star viewer built from the **Hipparcos New Reduction** catalog, with a local SQLite database and a fullscreen interactive renderer.

## What this project does

This project takes Hipparcos astrometric catalog data, converts the stars into 3D Cartesian positions centered on the Earth/Solar System observing location used by the catalog, stores the result in a local SQLite database, and renders the stars as white points in a fullscreen viewer.

Current viewer features:
- Fullscreen starfield viewer
- Mouse-look camera
- Stars rendered as white points on a black background
- Magnitude cutoff control with `[` and `]`
- Brighter stars rendered larger and more opaque than dimmer stars
- Data filtered to a higher-confidence 3D subset for position rendering

## Source data

The data comes from the CDS (Centre de Données astronomiques de Strasbourg) mirror/distribution of the Hipparcos New Reduction catalog.

Primary catalog page / README:
- `https://cdsarc.cds.unistra.fr/ftp/I/311/ReadMe`

Files used:
- `https://cdsarc.cds.unistra.fr/ftp/I/311/hip2.dat`
- `https://cdsarc.cds.unistra.fr/ftp/I/311/hip7p.dat`
- `https://cdsarc.cds.unistra.fr/ftp/I/311/hip9p.dat`
- `https://cdsarc.cds.unistra.fr/ftp/I/311/hipvim.dat`

Local copies currently in this folder:
- `I_311_hip2.dat.gz.fits` (FITS export of the main catalog table)
- `hip7p.dat`
- `hip9p.dat`
- `hipvim.dat`

## About the survey / catalog

This project uses **Hipparcos, the New Reduction of the Raw Data** by **Floor van Leeuwen**.

Reference from the CDS README:
- van Leeuwen F.
- *Astronomy & Astrophysics* **474**, 653 (2007)
- Bibcode: `2007A&A...474..653V`

### What Hipparcos was

Hipparcos was the European Space Agency's pioneering space astrometry mission. Its purpose was to measure precise stellar positions, parallaxes, and proper motions. The original Hipparcos catalog was released in the late 1990s.

### What the “New Reduction” is

The 2007 van Leeuwen work is a re-reduction of the original Hipparcos raw data. The aim was to improve the astrometric solution, reduce systematic issues, improve formal errors, and produce better parallaxes and proper motions.

The CDS README notes that this new reduction improved the total weight of the catalog significantly relative to the 1997 release and is especially useful for studies of stellar luminosities and local galactic kinematics.

### Supplemental solution files

The catalog is not only a simple 5-parameter star list. Some stars require additional astrometric solution types:
- `hip7p.dat` — seven-parameter solutions
- `hip9p.dat` — nine-parameter solutions
- `hipvim.dat` — VIM (variability-induced mover) solutions

These are linked to the main catalog through the Hipparcos star identifier (`HIP`).

## Files in this project

### Data files
- `I_311_hip2.dat.gz.fits` — FITS version of the main Hipparcos new reduction table
- `hip7p.dat` — 7-parameter supplement
- `hip9p.dat` — 9-parameter supplement
- `hipvim.dat` — VIM supplement
- `hipparcos.db` — local SQLite database created for this project

### Scripts
- `build_hipparcos_db.py` — builds the main SQLite database from the FITS file
- `merge_hipparcos_supplements.py` — downloads/parses supplemental catalogs and merges them into the database
- `star_viewer.py` — fullscreen 3D star viewer
- `run_star_viewer.cmd` — convenience launcher for the viewer on Windows

## How the database was made

### Step 1: inspect the FITS file
The FITS file was examined and identified as a table version of the main Hipparcos new reduction catalog (`hip2.dat`). It contains 117,955 rows and 41 fields.

### Step 2: build a SQLite database from the FITS table
`build_hipparcos_db.py` creates `hipparcos.db` and builds a main table called `stars`.

The script:
- reads the FITS table structure
- extracts all original catalog columns
- stores the original fields as text so the source content is preserved
- parses important numeric values into helper columns
- computes 3D coordinates from RA, Dec, and parallax
- stores filtering flags for 3D usability

### Main computed fields added
The `stars` table includes parsed numeric helper fields such as:
- `ra_rad_num`, `dec_rad_num`
- `ra_deg`, `dec_deg`
- `parallax_mas`, `parallax_error_mas`
- `distance_pc`
- `pmra_masyr`, `pmdec_masyr`
- `hpmag_num`, `bv_num`, `vi_num`
- `x`, `y`, `z`
- `parallax_over_error`
- `frac_parallax_error`
- `has_valid_3d`
- `excluded_from_3d`
- `exclusion_reason`

### How 3D coordinates were computed
The coordinates are derived from:
- right ascension in radians
- declination in radians
- parallax in milliarcseconds

Distance is computed as:
- `distance_pc = 1000 / parallax_mas`

Then Cartesian coordinates are computed as:
- `x = d * cos(dec) * cos(ra)`
- `y = d * cos(dec) * sin(ra)`
- `z = d * sin(dec)`

This yields a 3D Earth-centered star distribution in parsecs.

## Quality filtering used for 3D rendering
Not every entry is equally trustworthy for direct 3D plotting from parallax.

For the rendered 3D subset, the project currently uses this practical quality rule:
- exclude stars with non-positive parallax
- exclude stars with missing position data
- exclude stars with fractional parallax error greater than `0.2`

That is:
- exclude when `e_Plx / Plx > 0.2`

### Current counts
- Total stars in main catalog: **117,955**
- Excluded from 3D subset: **57,697**
- Kept for 3D rendering: **60,258**

This cutoff is a practical visualization choice for more stable 3D placement. It is not a claim that the excluded stars are invalid in general astronomy use.

## Solution-type decoding
The `Sn` field in the catalog encodes solution type information. This project decodes it into explicit columns in the `stars` table:
- `sn_solution_digit`
- `sn_solution_type`
- `sn_flag_double`
- `sn_flag_variable`
- `sn_flag_photocenter`
- `sn_flag_secondary`

Current decoded counts:
- 5-parameter: **115,112**
- 7-parameter: **1,338**
- 9-parameter: **104**
- stochastic: **1,371**
- vim: **25**
- unknown: **5**

## Supplemental catalog merge
`merge_hipparcos_supplements.py` does the following:
- downloads the supplemental files from CDS
- parses their fixed-width columns
- creates separate SQLite tables:
  - `hip7p`
  - `hip9p`
  - `hipvim`
- creates a convenience joined view called `stars_expanded`

### Supplemental row counts
- `hip7p`: **1,338** rows
- `hip9p`: **104** rows
- `hipvim`: **25** rows

## How the viewer works

### Entry point
Run:
- `run_star_viewer.cmd`

This launches:
- `star_viewer.py`

### Rendering approach
The viewer:
- opens a fullscreen OpenGL window using `pygame`
- uses `moderngl` for GPU-based rendering
- reads valid stars from `hipparcos.db`
- uses `hpmag_num` to control visibility and visual styling
- uses a mouse-look camera centered at the origin

### Magnitude visibility control
The viewer starts with magnitude limit **5.0**.

Controls:
- `]` increases the visible magnitude limit (shows dimmer stars too)
- `[` decreases the visible magnitude limit (shows only brighter stars)
- `Esc` or `Q` exits

### Brightness / size styling
The viewer uses `hpmag_num` to map stars visually:
- brighter stars appear larger
- brighter stars appear more opaque
- dimmer stars appear smaller and fainter

The magnitude range is clamped and compressed for visual clarity so the display is more useful than a literal raw mapping.

## Main dependencies
This project currently depends on Python packages available in the local environment, including:
- `numpy`
- `pygame`
- `moderngl`
- `pyrr`
- `astropy`

## Notes on interpretation
This is currently a visualization-first build, not a full scientific analysis pipeline. Some choices are intentionally practical for rendering:
- limiting to a higher-confidence 3D subset
- compressing brightness visually
- rendering all stars as white for now
- using Earth-centered apparent magnitudes for display

## Suggested future improvements
- add an on-screen HUD for current magnitude limit
- add color from `B-V` or `V-I`
- add labels or info on selected stars
- allow free movement through space
- add LOD / precomputed magnitude tiers for even faster updates
- add export scripts for game-engine or WebGL pipelines

## Repository provenance
This README was written after building the local database, supplement merge, and interactive viewer in this folder. It documents the current state of the project as implemented here.
