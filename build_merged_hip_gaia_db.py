import json
import os
import sqlite3
from pathlib import Path

import pyarrow.parquet as pq

HIP_DB = Path(r"D:\OC\spaceSim\hipparcos.db")
GAIA_STARS_DIR = Path(r"D:\OC\spaceSim\gaia_extract\data\particles_000000\stars")
GAIA_ALIASES_DIR = Path(r"D:\OC\spaceSim\gaia_extract\data\particles_000000\aliases")
OUT_DB = Path(r"D:\OC\spaceSim\hip_gaia_merged.db")


def q(name: str) -> str:
    return '[' + name.replace(']', ']]') + ']'


def blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == '')


def parse_int(value):
    if blank(value):
        return None
    try:
        return int(value)
    except Exception:
        return None


def textify(value):
    if value is None:
        return None
    return str(value)


def coalesce(*values):
    for value in values:
        if not blank(value):
            return value
    return None


def compute_display_name(common_name, hip_id, gaia_hip_name, gaia_name, source_id):
    common_name = coalesce(common_name)
    if common_name:
        return common_name
    if hip_id is not None:
        return f"HIP {hip_id}"
    if gaia_hip_name:
        return gaia_hip_name
    if gaia_name:
        return gaia_name
    if source_id is not None:
        return f"Source {source_id}"
    return "Unnamed star"


def build_schema(hip_conn: sqlite3.Connection, out_conn: sqlite3.Connection):
    hip_cols = hip_conn.execute("PRAGMA table_info(stars)").fetchall()
    hip_defs = []
    hip_names = []
    for _, name, col_type, notnull, default_value, _ in hip_cols:
        if name == 'id':
            continue
        hip_names.append(name)
        col_sql = f"{q(name)} {col_type or 'TEXT'}"
        if notnull:
            col_sql += " NOT NULL"
        if default_value is not None:
            col_sql += f" DEFAULT {default_value}"
        hip_defs.append(col_sql)

    extra_defs = [
        "merged_row_id INTEGER PRIMARY KEY AUTOINCREMENT",
        "hip_db_id INTEGER",
        "merge_key TEXT NOT NULL UNIQUE",
        "has_hip INTEGER NOT NULL DEFAULT 0",
        "has_gaia INTEGER NOT NULL DEFAULT 0",
        "preferred_catalog TEXT",
        "hip_id_int INTEGER",
        "gaia_source_id INTEGER",
        "gaia_source_id_kind TEXT",
        "gaia_name TEXT",
        "gaia_hip_name TEXT",
        "gaia_common_name TEXT",
        "gaia_raw_name TEXT",
        "gaia_record_index INTEGER",
        "gaia_source_file TEXT",
        "gaia_distance_pc REAL",
        "gaia_ra_rad REAL",
        "gaia_dec_rad REAL",
        "gaia_ra_deg REAL",
        "gaia_dec_deg REAL",
        "gaia_x REAL",
        "gaia_y REAL",
        "gaia_z REAL",
        "gaia_mu_alpha_masyr REAL",
        "gaia_mu_delta_masyr REAL",
        "gaia_radial_velocity_km_s REAL",
        "gaia_apparent_magnitude REAL",
        "gaia_absolute_magnitude REAL",
        "gaia_temperature_k REAL",
        "gaia_color_r REAL",
        "gaia_color_g REAL",
        "gaia_color_b REAL",
        "gaia_has_valid_3d INTEGER",
        "gaia_excluded_from_3d INTEGER",
        "gaia_exclusion_reason TEXT",
        "merged_display_name TEXT",
        "merged_common_name TEXT",
        "merged_apparent_magnitude REAL",
        "merged_absolute_magnitude REAL",
        "merged_radial_velocity_km_s REAL",
    ]

    out_conn.execute("DROP TABLE IF EXISTS stars")
    out_conn.execute("DROP TABLE IF EXISTS gaia_aliases")
    out_conn.execute("DROP TABLE IF EXISTS merge_stats")
    out_conn.execute(f"CREATE TABLE stars ({', '.join(extra_defs + hip_defs)})")
    out_conn.execute(
        "CREATE TABLE gaia_aliases (merge_key TEXT NOT NULL, gaia_source_id INTEGER, hip_id INTEGER, alias TEXT NOT NULL, alias_type TEXT, alias_order INTEGER)"
    )
    out_conn.execute("CREATE TABLE merge_stats (key TEXT PRIMARY KEY, value TEXT)")
    return hip_names


def insert_hip_rows(hip_conn: sqlite3.Connection, out_conn: sqlite3.Connection, hip_names):
    hip_conn.row_factory = sqlite3.Row
    rows = hip_conn.execute("SELECT * FROM stars").fetchall()

    insert_cols = [
        'hip_db_id', 'merge_key', 'has_hip', 'has_gaia', 'preferred_catalog',
        'hip_id_int', 'merged_display_name', 'merged_common_name',
        'merged_apparent_magnitude', 'merged_absolute_magnitude', 'merged_radial_velocity_km_s',
    ] + hip_names
    insert_sql = f"INSERT INTO stars ({', '.join(q(c) for c in insert_cols)}) VALUES ({', '.join('?' for _ in insert_cols)})"

    existing = {}
    batch = []
    for row in rows:
        row_dict = dict(row)
        hip_id_int = parse_int(row_dict.get('HIP'))
        merge_key = f"hip:{hip_id_int}" if hip_id_int is not None else f"hiprow:{row_dict['id']}"
        merged_common_name = coalesce(row_dict.get('common_name'))
        merged_display_name = compute_display_name(merged_common_name, hip_id_int, None, None, None)
        merged_app_mag = row_dict.get('hpmag_num')
        values = [
            row_dict['id'], merge_key, 1, 0, 'hipparcos', hip_id_int,
            merged_display_name, merged_common_name,
            merged_app_mag, None, None,
        ] + [row_dict.get(name) for name in hip_names]
        batch.append(values)
        existing[merge_key] = {
            'hip_db_id': row_dict['id'],
            'hip_id_int': hip_id_int,
            'HIP': row_dict.get('HIP'),
            'common_name': row_dict.get('common_name'),
            'ra_deg': row_dict.get('ra_deg'),
            'dec_deg': row_dict.get('dec_deg'),
            'ra_rad_num': row_dict.get('ra_rad_num'),
            'dec_rad_num': row_dict.get('dec_rad_num'),
            'distance_pc': row_dict.get('distance_pc'),
            'pmra_masyr': row_dict.get('pmra_masyr'),
            'pmdec_masyr': row_dict.get('pmdec_masyr'),
            'x': row_dict.get('x'),
            'y': row_dict.get('y'),
            'z': row_dict.get('z'),
            'temperature_k': row_dict.get('temperature_k'),
            'color_r': row_dict.get('color_r'),
            'color_g': row_dict.get('color_g'),
            'color_b': row_dict.get('color_b'),
            'has_valid_3d': row_dict.get('has_valid_3d'),
            'excluded_from_3d': row_dict.get('excluded_from_3d'),
            'exclusion_reason': row_dict.get('exclusion_reason'),
            'hpmag_num': row_dict.get('hpmag_num'),
        }

        if len(batch) >= 1000:
            out_conn.executemany(insert_sql, batch)
            out_conn.commit()
            batch = []

    if batch:
        out_conn.executemany(insert_sql, batch)
        out_conn.commit()
    return existing, len(rows)


def merge_gaia_rows(out_conn: sqlite3.Connection, existing: dict):
    update_sql = """
    UPDATE stars SET
        has_gaia = 1,
        preferred_catalog = 'hipparcos',
        gaia_source_id = ?,
        gaia_source_id_kind = ?,
        gaia_name = ?,
        gaia_hip_name = ?,
        gaia_common_name = ?,
        gaia_raw_name = ?,
        gaia_record_index = ?,
        gaia_source_file = ?,
        gaia_distance_pc = ?,
        gaia_ra_rad = ?,
        gaia_dec_rad = ?,
        gaia_ra_deg = ?,
        gaia_dec_deg = ?,
        gaia_x = ?,
        gaia_y = ?,
        gaia_z = ?,
        gaia_mu_alpha_masyr = ?,
        gaia_mu_delta_masyr = ?,
        gaia_radial_velocity_km_s = ?,
        gaia_apparent_magnitude = ?,
        gaia_absolute_magnitude = ?,
        gaia_temperature_k = ?,
        gaia_color_r = ?,
        gaia_color_g = ?,
        gaia_color_b = ?,
        gaia_has_valid_3d = ?,
        gaia_excluded_from_3d = ?,
        gaia_exclusion_reason = ?,
        HIP = COALESCE(HIP, ?),
        common_name = COALESCE(common_name, ?),
        RArad = COALESCE(RArad, ?),
        DErad = COALESCE(DErad, ?),
        pmRA = COALESCE(pmRA, ?),
        pmDE = COALESCE(pmDE, ?),
        ra_rad_num = COALESCE(ra_rad_num, ?),
        dec_rad_num = COALESCE(dec_rad_num, ?),
        ra_deg = COALESCE(ra_deg, ?),
        dec_deg = COALESCE(dec_deg, ?),
        distance_pc = COALESCE(distance_pc, ?),
        pmra_masyr = COALESCE(pmra_masyr, ?),
        pmdec_masyr = COALESCE(pmdec_masyr, ?),
        x = COALESCE(x, ?),
        y = COALESCE(y, ?),
        z = COALESCE(z, ?),
        temperature_k = COALESCE(temperature_k, ?),
        color_r = COALESCE(color_r, ?),
        color_g = COALESCE(color_g, ?),
        color_b = COALESCE(color_b, ?),
        exclusion_reason = COALESCE(exclusion_reason, ?),
        merged_common_name = COALESCE(common_name, ?, gaia_common_name),
        merged_display_name = ?,
        merged_apparent_magnitude = COALESCE(hpmag_num, ?),
        merged_absolute_magnitude = ?,
        merged_radial_velocity_km_s = ?
    WHERE merge_key = ?
    """

    insert_cols = [
        'hip_db_id', 'merge_key', 'has_hip', 'has_gaia', 'preferred_catalog', 'hip_id_int',
        'HIP', 'common_name', 'RArad', 'DErad', 'pmRA', 'pmDE',
        'ra_rad_num', 'dec_rad_num', 'ra_deg', 'dec_deg', 'distance_pc',
        'pmra_masyr', 'pmdec_masyr', 'x', 'y', 'z',
        'temperature_k', 'color_r', 'color_g', 'color_b',
        'has_valid_3d', 'excluded_from_3d', 'exclusion_reason',
        'gaia_source_id', 'gaia_source_id_kind', 'gaia_name', 'gaia_hip_name', 'gaia_common_name', 'gaia_raw_name',
        'gaia_record_index', 'gaia_source_file', 'gaia_distance_pc', 'gaia_ra_rad', 'gaia_dec_rad', 'gaia_ra_deg', 'gaia_dec_deg',
        'gaia_x', 'gaia_y', 'gaia_z', 'gaia_mu_alpha_masyr', 'gaia_mu_delta_masyr', 'gaia_radial_velocity_km_s',
        'gaia_apparent_magnitude', 'gaia_absolute_magnitude', 'gaia_temperature_k', 'gaia_color_r', 'gaia_color_g', 'gaia_color_b',
        'gaia_has_valid_3d', 'gaia_excluded_from_3d', 'gaia_exclusion_reason',
        'merged_display_name', 'merged_common_name', 'merged_apparent_magnitude', 'merged_absolute_magnitude', 'merged_radial_velocity_km_s'
    ]
    insert_sql = f"INSERT INTO stars ({', '.join(q(c) for c in insert_cols)}) VALUES ({', '.join('?' for _ in insert_cols)})"

    cols = [
        'source_file', 'record_index', 'source_id', 'source_id_kind', 'gaia_source_id', 'gaia_name', 'hip_id', 'hip_name', 'common_name', 'raw_name',
        'distance_pc', 'ra_rad', 'dec_rad', 'ra_deg', 'dec_deg', 'x_eq_pc', 'y_eq_pc', 'z_eq_pc',
        'mu_alpha_mas_per_year', 'mu_delta_mas_per_year', 'radial_velocity_km_per_s', 'apparent_magnitude', 'absolute_magnitude',
        'temperature_k', 'color_r', 'color_g', 'color_b', 'has_valid_3d', 'excluded_from_3d', 'exclusion_reason'
    ]

    overlap_count = 0
    gaia_only_count = 0
    total_gaia = 0
    for part in sorted(GAIA_STARS_DIR.glob('*.parquet')):
        table = pq.read_table(part, columns=cols)
        for row in table.to_pylist():
            total_gaia += 1
            hip_id = row['hip_id']
            source_id = row['gaia_source_id'] if row['gaia_source_id'] is not None else row['source_id']
            merge_key = f"hip:{int(hip_id)}" if hip_id is not None else f"gaia:{int(source_id)}"
            gaia_common_name = coalesce(row.get('common_name'))
            gaia_hip_name = coalesce(row.get('hip_name'))
            gaia_name = coalesce(row.get('gaia_name'))
            display_name = compute_display_name(gaia_common_name, hip_id, gaia_hip_name, gaia_name, source_id)
            hip_text = textify(hip_id)
            gaia_has_valid = 1 if row.get('has_valid_3d') else 0 if row.get('has_valid_3d') is not None else None
            gaia_excluded = 1 if row.get('excluded_from_3d') else 0 if row.get('excluded_from_3d') is not None else None

            if merge_key in existing:
                overlap_count += 1
                out_conn.execute(update_sql, (
                    source_id,
                    row.get('source_id_kind'),
                    gaia_name,
                    gaia_hip_name,
                    gaia_common_name,
                    row.get('raw_name'),
                    row.get('record_index'),
                    row.get('source_file'),
                    row.get('distance_pc'),
                    row.get('ra_rad'),
                    row.get('dec_rad'),
                    row.get('ra_deg'),
                    row.get('dec_deg'),
                    row.get('x_eq_pc'),
                    row.get('y_eq_pc'),
                    row.get('z_eq_pc'),
                    row.get('mu_alpha_mas_per_year'),
                    row.get('mu_delta_mas_per_year'),
                    row.get('radial_velocity_km_per_s'),
                    row.get('apparent_magnitude'),
                    row.get('absolute_magnitude'),
                    row.get('temperature_k'),
                    row.get('color_r'),
                    row.get('color_g'),
                    row.get('color_b'),
                    gaia_has_valid,
                    gaia_excluded,
                    row.get('exclusion_reason'),
                    hip_text,
                    gaia_common_name,
                    textify(row.get('ra_rad')),
                    textify(row.get('dec_rad')),
                    textify(row.get('mu_alpha_mas_per_year')),
                    textify(row.get('mu_delta_mas_per_year')),
                    row.get('ra_rad'),
                    row.get('dec_rad'),
                    row.get('ra_deg'),
                    row.get('dec_deg'),
                    row.get('distance_pc'),
                    row.get('mu_alpha_mas_per_year'),
                    row.get('mu_delta_mas_per_year'),
                    row.get('x_eq_pc'),
                    row.get('y_eq_pc'),
                    row.get('z_eq_pc'),
                    row.get('temperature_k'),
                    row.get('color_r'),
                    row.get('color_g'),
                    row.get('color_b'),
                    row.get('exclusion_reason'),
                    gaia_common_name,
                    compute_display_name(
                        coalesce(existing[merge_key].get('common_name'), gaia_common_name),
                        existing[merge_key].get('hip_id_int') if existing[merge_key].get('hip_id_int') is not None else hip_id,
                        gaia_hip_name,
                        gaia_name,
                        source_id,
                    ),
                    row.get('apparent_magnitude'),
                    row.get('absolute_magnitude'),
                    row.get('radial_velocity_km_per_s'),
                    merge_key,
                ))
            else:
                gaia_only_count += 1
                values = [
                    None, merge_key, 0, 1, 'gaia', hip_id,
                    hip_text, gaia_common_name, textify(row.get('ra_rad')), textify(row.get('dec_rad')), textify(row.get('mu_alpha_mas_per_year')), textify(row.get('mu_delta_mas_per_year')),
                    row.get('ra_rad'), row.get('dec_rad'), row.get('ra_deg'), row.get('dec_deg'), row.get('distance_pc'),
                    row.get('mu_alpha_mas_per_year'), row.get('mu_delta_mas_per_year'), row.get('x_eq_pc'), row.get('y_eq_pc'), row.get('z_eq_pc'),
                    row.get('temperature_k'), row.get('color_r'), row.get('color_g'), row.get('color_b'),
                    gaia_has_valid, gaia_excluded, row.get('exclusion_reason'),
                    source_id, row.get('source_id_kind'), gaia_name, gaia_hip_name, gaia_common_name, row.get('raw_name'),
                    row.get('record_index'), row.get('source_file'), row.get('distance_pc'), row.get('ra_rad'), row.get('dec_rad'), row.get('ra_deg'), row.get('dec_deg'),
                    row.get('x_eq_pc'), row.get('y_eq_pc'), row.get('z_eq_pc'), row.get('mu_alpha_mas_per_year'), row.get('mu_delta_mas_per_year'), row.get('radial_velocity_km_per_s'),
                    row.get('apparent_magnitude'), row.get('absolute_magnitude'), row.get('temperature_k'), row.get('color_r'), row.get('color_g'), row.get('color_b'),
                    gaia_has_valid, gaia_excluded, row.get('exclusion_reason'),
                    display_name, gaia_common_name, row.get('apparent_magnitude'), row.get('absolute_magnitude'), row.get('radial_velocity_km_per_s'),
                ]
                out_conn.execute(insert_sql, values)
                existing[merge_key] = {
                    'hip_db_id': None,
                    'hip_id_int': hip_id,
                    'HIP': hip_text,
                    'common_name': gaia_common_name,
                    'ra_deg': row.get('ra_deg'),
                    'dec_deg': row.get('dec_deg'),
                    'ra_rad_num': row.get('ra_rad'),
                    'dec_rad_num': row.get('dec_rad'),
                    'distance_pc': row.get('distance_pc'),
                    'pmra_masyr': row.get('mu_alpha_mas_per_year'),
                    'pmdec_masyr': row.get('mu_delta_mas_per_year'),
                    'x': row.get('x_eq_pc'),
                    'y': row.get('y_eq_pc'),
                    'z': row.get('z_eq_pc'),
                    'temperature_k': row.get('temperature_k'),
                    'color_r': row.get('color_r'),
                    'color_g': row.get('color_g'),
                    'color_b': row.get('color_b'),
                    'has_valid_3d': gaia_has_valid,
                    'excluded_from_3d': gaia_excluded,
                    'exclusion_reason': row.get('exclusion_reason'),
                    'hpmag_num': None,
                }

        out_conn.commit()

    return total_gaia, overlap_count, gaia_only_count


def copy_gaia_aliases(out_conn: sqlite3.Connection):
    if not GAIA_ALIASES_DIR.exists():
        return 0
    total = 0
    insert_sql = "INSERT INTO gaia_aliases (merge_key, gaia_source_id, hip_id, alias, alias_type, alias_order) VALUES (?, ?, ?, ?, ?, ?)"
    for part in sorted(GAIA_ALIASES_DIR.glob('*.parquet')):
        table = pq.read_table(part, columns=['source_id', 'alias', 'alias_type', 'alias_order'])
        batch = []
        for row in table.to_pylist():
            source_id = int(row['source_id'])
            merge_key = f"gaia:{source_id}"
            batch.append((merge_key, source_id, None, row['alias'], row.get('alias_type'), row.get('alias_order')))
        if batch:
            out_conn.executemany(insert_sql, batch)
            out_conn.commit()
            total += len(batch)

    out_conn.execute(
        '''UPDATE gaia_aliases
           SET merge_key = COALESCE((SELECT s.merge_key FROM stars s WHERE s.gaia_source_id = gaia_aliases.gaia_source_id LIMIT 1), merge_key),
               hip_id = (SELECT s.hip_id_int FROM stars s WHERE s.gaia_source_id = gaia_aliases.gaia_source_id LIMIT 1)'''
    )
    out_conn.commit()
    return total


def copy_hip_supplements(hip_conn: sqlite3.Connection, out_conn: sqlite3.Connection):
    tables = ['hip7p', 'hip9p', 'hipvim']
    copied = {}
    for table_name in tables:
        exists = hip_conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
        if not exists:
            continue
        rows = hip_conn.execute(f"SELECT * FROM {table_name}").fetchall()
        cols = hip_conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        col_defs = []
        col_names = []
        for _, name, col_type, notnull, default_value, pk in cols:
            part = f"{q(name)} {col_type or 'TEXT'}"
            if pk:
                part += ' PRIMARY KEY'
            elif notnull:
                part += ' NOT NULL'
            if default_value is not None:
                part += f" DEFAULT {default_value}"
            col_defs.append(part)
            col_names.append(name)
        out_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        out_conn.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")
        if rows:
            placeholders = ', '.join('?' for _ in col_names)
            out_conn.executemany(
                f"INSERT INTO {table_name} ({', '.join(q(c) for c in col_names)}) VALUES ({placeholders})",
                rows,
            )
        copied[table_name] = len(rows)
    out_conn.execute("DROP VIEW IF EXISTS stars_expanded")
    out_conn.execute(
        '''CREATE VIEW stars_expanded AS
           SELECT s.*,
                  h7.Fg AS h7_Fg, h7.dpmRA AS h7_dpmRA, h7.dpmDE AS h7_dpmDE, h7.e_dpmRA AS h7_e_dpmRA, h7.e_dpmDE AS h7_e_dpmDE, h7.UW AS h7_UW,
                  h9.Fg AS h9_Fg, h9.dpmRA AS h9_dpmRA, h9.dpmDE AS h9_dpmDE, h9.ddpmRA AS h9_ddpmRA, h9.ddpmDE AS h9_ddpmDE, h9.e_dpmRA AS h9_e_dpmRA, h9.e_dpmDE AS h9_e_dpmDE, h9.e_ddpmRA AS h9_e_ddpmRA, h9.e_ddpmDE AS h9_e_ddpmDE, h9.UW AS h9_UW,
                  hv.Fg AS hv_Fg, hv.upsRA AS hv_upsRA, hv.upsDE AS hv_upsDE, hv.e_upsRA AS hv_e_upsRA, hv.e_upsDE AS hv_e_upsDE, hv.UW AS hv_UW
           FROM stars s
           LEFT JOIN hip7p h7 ON h7.HIP = s.HIP
           LEFT JOIN hip9p h9 ON h9.HIP = s.HIP
           LEFT JOIN hipvim hv ON hv.HIP = s.HIP'''
    )
    out_conn.commit()
    return copied


def create_indexes(out_conn: sqlite3.Connection):
    index_sql = [
        "CREATE INDEX IF NOT EXISTS idx_stars_merge_key ON stars(merge_key)",
        "CREATE INDEX IF NOT EXISTS idx_stars_hip ON stars(HIP)",
        "CREATE INDEX IF NOT EXISTS idx_stars_hip_id_int ON stars(hip_id_int)",
        "CREATE INDEX IF NOT EXISTS idx_stars_gaia_source_id ON stars(gaia_source_id)",
        "CREATE INDEX IF NOT EXISTS idx_stars_sources ON stars(has_hip, has_gaia)",
        "CREATE INDEX IF NOT EXISTS idx_stars_display_name ON stars(merged_display_name)",
        "CREATE INDEX IF NOT EXISTS idx_stars_xyz ON stars(x, y, z)",
        "CREATE INDEX IF NOT EXISTS idx_gaia_aliases_merge_key ON gaia_aliases(merge_key)",
        "CREATE INDEX IF NOT EXISTS idx_gaia_aliases_alias ON gaia_aliases(alias)",
    ]
    for sql in index_sql:
        out_conn.execute(sql)
    out_conn.commit()


def store_stats(out_conn: sqlite3.Connection, stats: dict):
    out_conn.executemany(
        "INSERT OR REPLACE INTO merge_stats (key, value) VALUES (?, ?)",
        [(k, json.dumps(v) if not isinstance(v, str) else v) for k, v in stats.items()],
    )
    out_conn.commit()


def main():
    if OUT_DB.exists():
        os.remove(OUT_DB)

    hip_conn = sqlite3.connect(HIP_DB)
    hip_conn.row_factory = sqlite3.Row
    out_conn = sqlite3.connect(OUT_DB)
    out_conn.row_factory = sqlite3.Row
    out_conn.execute('PRAGMA journal_mode=WAL')
    out_conn.execute('PRAGMA synchronous=NORMAL')

    hip_names = build_schema(hip_conn, out_conn)
    existing, hip_count = insert_hip_rows(hip_conn, out_conn, hip_names)
    gaia_total, overlap_count, gaia_only_count = merge_gaia_rows(out_conn, existing)
    alias_count = copy_gaia_aliases(out_conn)
    supplement_counts = copy_hip_supplements(hip_conn, out_conn)
    create_indexes(out_conn)

    final_count = out_conn.execute('SELECT COUNT(*) FROM stars').fetchone()[0]
    hip_only = out_conn.execute('SELECT COUNT(*) FROM stars WHERE has_hip = 1 AND has_gaia = 0').fetchone()[0]
    gaia_only = out_conn.execute('SELECT COUNT(*) FROM stars WHERE has_hip = 0 AND has_gaia = 1').fetchone()[0]
    both = out_conn.execute('SELECT COUNT(*) FROM stars WHERE has_hip = 1 AND has_gaia = 1').fetchone()[0]
    sample = [dict(r) for r in out_conn.execute(
        'SELECT merge_key, HIP, hip_id_int, gaia_source_id, merged_display_name, common_name, gaia_common_name, hpmag_num, merged_apparent_magnitude FROM stars ORDER BY has_hip DESC, has_gaia DESC, merged_display_name LIMIT 10'
    ).fetchall()]

    stats = {
        'hip_input_count': hip_count,
        'gaia_input_count': gaia_total,
        'overlap_count': overlap_count,
        'gaia_only_inserted': gaia_only_count,
        'final_star_count': final_count,
        'hip_only_count': hip_only,
        'gaia_only_count': gaia_only,
        'both_count': both,
        'gaia_alias_count': alias_count,
        'supplement_counts': supplement_counts,
        'output_db': str(OUT_DB),
        'sample': sample,
    }
    store_stats(out_conn, stats)
    print(json.dumps(stats, indent=2))

    out_conn.close()
    hip_conn.close()


if __name__ == '__main__':
    main()
