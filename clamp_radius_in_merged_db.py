import math
import sqlite3

DB = r'D:\OC\spaceSim\hip_gaia_merged.db'
R_SUN_M = 6.957e8


def pick_abs_mag(merged_abs, gaia_abs, merged_app, distance_pc):
    if merged_abs is not None and math.isfinite(float(merged_abs)):
        return float(merged_abs)
    if gaia_abs is not None and math.isfinite(float(gaia_abs)):
        return float(gaia_abs)
    if merged_app is not None and distance_pc is not None:
        m = float(merged_app)
        d = float(distance_pc)
        if math.isfinite(m) and math.isfinite(d) and d > 0:
            return m - 5.0 * math.log10(d / 10.0)
    return None


def pick_temp(temp_k, gaia_temp_k):
    if temp_k is not None and math.isfinite(float(temp_k)) and float(temp_k) > 0:
        return float(temp_k)
    if gaia_temp_k is not None and math.isfinite(float(gaia_temp_k)) and float(gaia_temp_k) > 0:
        return float(gaia_temp_k)
    return None


def classify_star(abs_mag, temp_k):
    if abs_mag is None or temp_k is None:
        return 'unknown'
    if abs_mag > 10 and temp_k >= 5000:
        return 'white_dwarf'
    if abs_mag <= -5:
        return 'supergiant'
    if abs_mag <= 0:
        return 'giant'
    if abs_mag <= 3 and 4500 <= temp_k <= 8000:
        return 'subgiant'
    return 'main_sequence'


def class_profile(star_class, temp_k):
    if star_class == 'white_dwarf':
        return 0.013, 0.005, 0.05
    if star_class == 'supergiant':
        if temp_k is None:
            return 80.0, 20.0, 1500.0
        if temp_k < 4500:
            return 600.0, 100.0, 1500.0
        if temp_k < 8000:
            return 150.0, 30.0, 1500.0
        return 25.0, 8.0, 1500.0
    if star_class == 'giant':
        if temp_k is None:
            return 15.0, 4.0, 1500.0
        if temp_k < 4500:
            return 35.0, 8.0, 1500.0
        if temp_k < 8000:
            return 10.0, 3.0, 1500.0
        return 6.0, 2.0, 1500.0
    if star_class == 'subgiant':
        return 2.5, 1.5, 1500.0
    if star_class == 'main_sequence':
        if temp_k is None:
            return 1.0, 0.08, 1500.0
        if temp_k >= 30000:
            return 7.0, 3.0, 1500.0
        if temp_k >= 10000:
            return 2.5, 1.3, 1500.0
        if temp_k >= 7500:
            return 1.7, 1.1, 1500.0
        if temp_k >= 6000:
            return 1.15, 0.85, 1500.0
        if temp_k >= 5200:
            return 0.95, 0.75, 1500.0
        if temp_k >= 3700:
            return 0.75, 0.4, 1500.0
        return 0.35, 0.08, 1500.0
    return 1.0, 0.08, 1500.0


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for sql in [
        'ALTER TABLE stars ADD COLUMN radius_rsun_raw REAL',
        'ALTER TABLE stars ADD COLUMN radius_m_raw REAL',
        'ALTER TABLE stars ADD COLUMN radius_class TEXT',
        'ALTER TABLE stars ADD COLUMN radius_bounds_min_rsun REAL',
        'ALTER TABLE stars ADD COLUMN radius_bounds_max_rsun REAL',
        'ALTER TABLE stars ADD COLUMN radius_is_fallback INTEGER',
        'ALTER TABLE stars ADD COLUMN radius_clamp_reason TEXT',
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    cur.execute('UPDATE stars SET radius_rsun_raw = COALESCE(radius_rsun_raw, radius_rsun), radius_m_raw = COALESCE(radius_m_raw, radius_m)')
    conn.commit()

    rows = cur.execute(
        'SELECT merged_row_id, radius_rsun_raw, merged_absolute_magnitude, gaia_absolute_magnitude, merged_apparent_magnitude, distance_pc, temperature_k, gaia_temperature_k FROM stars'
    ).fetchall()

    batch = []
    fallback_count = 0
    kept_count = 0
    unknown_count = 0
    for row_id, raw_radius, merged_abs, gaia_abs, merged_app, distance_pc, temp_k, gaia_temp_k in rows:
        abs_mag = pick_abs_mag(merged_abs, gaia_abs, merged_app, distance_pc)
        temp = pick_temp(temp_k, gaia_temp_k)
        star_class = classify_star(abs_mag, temp)
        typical, min_r, max_r = class_profile(star_class, temp)

        final_radius = raw_radius
        is_fallback = 0
        reason = None

        invalid_raw = (
            raw_radius is None or
            not math.isfinite(float(raw_radius)) or
            float(raw_radius) <= 0
        )

        if invalid_raw:
            final_radius = typical
            is_fallback = 1
            reason = 'missing_or_nonpositive_raw_radius'
        else:
            raw_radius = float(raw_radius)
            if raw_radius < min_r or raw_radius > max_r:
                final_radius = typical
                is_fallback = 1
                reason = f'outside_class_bounds:{raw_radius:.6g} not in [{min_r:.6g},{max_r:.6g}]'
            else:
                final_radius = raw_radius

        if star_class == 'unknown':
            unknown_count += 1

        if is_fallback:
            fallback_count += 1
        else:
            kept_count += 1

        radius_m = final_radius * R_SUN_M if final_radius is not None else None
        batch.append((
            final_radius,
            radius_m,
            star_class,
            min_r,
            max_r,
            is_fallback,
            reason,
            row_id,
        ))

        if len(batch) >= 1000:
            cur.executemany(
                'UPDATE stars SET radius_rsun=?, radius_m=?, radius_class=?, radius_bounds_min_rsun=?, radius_bounds_max_rsun=?, radius_is_fallback=?, radius_clamp_reason=? WHERE merged_row_id=?',
                batch,
            )
            conn.commit()
            batch = []

    if batch:
        cur.executemany(
            'UPDATE stars SET radius_rsun=?, radius_m=?, radius_class=?, radius_bounds_min_rsun=?, radius_bounds_max_rsun=?, radius_is_fallback=?, radius_clamp_reason=? WHERE merged_row_id=?',
            batch,
        )
        conn.commit()

    print('total_rows', cur.execute('SELECT COUNT(*) FROM stars').fetchone()[0])
    print('fallback_count', fallback_count)
    print('kept_count', kept_count)
    print('unknown_count', unknown_count)
    print('class_counts', cur.execute('SELECT radius_class, COUNT(*) FROM stars GROUP BY radius_class ORDER BY COUNT(*) DESC').fetchall())
    print('range_final', cur.execute('SELECT MIN(radius_rsun), MAX(radius_rsun), AVG(radius_rsun) FROM stars WHERE radius_rsun IS NOT NULL').fetchone())
    print('range_raw', cur.execute('SELECT MIN(radius_rsun_raw), MAX(radius_rsun_raw), AVG(radius_rsun_raw) FROM stars WHERE radius_rsun_raw IS NOT NULL').fetchone())
    print('fallback_examples')
    for row in cur.execute('SELECT merged_display_name, radius_class, radius_rsun_raw, radius_rsun, radius_clamp_reason FROM stars WHERE radius_is_fallback = 1 ORDER BY merged_apparent_magnitude ASC LIMIT 20'):
        print(row)

    conn.close()


if __name__ == '__main__':
    main()
