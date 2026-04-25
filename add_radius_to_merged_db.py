import math
import sqlite3

DB = r'D:\OC\spaceSim\hip_gaia_merged.db'
R_SUN_M = 6.957e8
M_SUN_V = 4.83
T_SUN_K = 5772.0
PC_TO_M = 3.085677581491367e16


def ballesteros_temperature(bv):
    return 4600.0 * ((1.0 / (0.92 * bv + 1.7)) + (1.0 / (0.92 * bv + 0.62)))


def choose_temperature(row):
    temperature_k, gaia_temperature_k, bv_num = row
    if temperature_k is not None and math.isfinite(float(temperature_k)) and float(temperature_k) > 0:
        return float(temperature_k), 'temperature_k'
    if gaia_temperature_k is not None and math.isfinite(float(gaia_temperature_k)) and float(gaia_temperature_k) > 0:
        return float(gaia_temperature_k), 'gaia_temperature_k'
    if bv_num is not None:
        try:
            temp = ballesteros_temperature(float(bv_num))
            if math.isfinite(temp) and temp > 0:
                return temp, 'bv_num_ballesteros'
        except Exception:
            pass
    return None, None


def choose_absolute_magnitude(row):
    merged_absolute_magnitude, gaia_absolute_magnitude, merged_apparent_magnitude, distance_pc = row
    if merged_absolute_magnitude is not None and math.isfinite(float(merged_absolute_magnitude)):
        return float(merged_absolute_magnitude), 'merged_absolute_magnitude'
    if gaia_absolute_magnitude is not None and math.isfinite(float(gaia_absolute_magnitude)):
        return float(gaia_absolute_magnitude), 'gaia_absolute_magnitude'
    if merged_apparent_magnitude is not None and distance_pc is not None:
        try:
            m = float(merged_apparent_magnitude)
            d = float(distance_pc)
            if d > 0 and math.isfinite(m) and math.isfinite(d):
                return m - 5.0 * math.log10(d / 10.0), 'apparent_mag_distance'
        except Exception:
            pass
    return None, None


def estimate_radius(abs_mag, temp_k):
    lum = 10.0 ** (-0.4 * (abs_mag - M_SUN_V))
    radius_rsun = math.sqrt(lum) * ((T_SUN_K / temp_k) ** 2)
    return lum, radius_rsun


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for sql in [
        'ALTER TABLE stars ADD COLUMN radius_rsun REAL',
        'ALTER TABLE stars ADD COLUMN radius_m REAL',
        'ALTER TABLE stars ADD COLUMN luminosity_lsun REAL',
        'ALTER TABLE stars ADD COLUMN radius_method TEXT',
        'ALTER TABLE stars ADD COLUMN radius_temp_source TEXT',
        'ALTER TABLE stars ADD COLUMN radius_mag_source TEXT',
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    rows = cur.execute(
        'SELECT merged_row_id, temperature_k, gaia_temperature_k, bv_num, merged_absolute_magnitude, gaia_absolute_magnitude, merged_apparent_magnitude, distance_pc FROM stars'
    ).fetchall()

    batch = []
    populated = 0
    for row_id, temperature_k, gaia_temperature_k, bv_num, merged_abs, gaia_abs, merged_app, distance_pc in rows:
        temp, temp_source = choose_temperature((temperature_k, gaia_temperature_k, bv_num))
        abs_mag, mag_source = choose_absolute_magnitude((merged_abs, gaia_abs, merged_app, distance_pc))

        radius_rsun = None
        radius_m = None
        luminosity_lsun = None
        method = None
        if temp is not None and abs_mag is not None:
            try:
                luminosity_lsun, radius_rsun = estimate_radius(abs_mag, temp)
                if math.isfinite(radius_rsun) and radius_rsun > 0 and math.isfinite(luminosity_lsun) and luminosity_lsun > 0:
                    radius_m = radius_rsun * R_SUN_M
                    method = 'mag_temp_stefan_boltzmann'
                    populated += 1
                else:
                    radius_rsun = radius_m = luminosity_lsun = None
            except Exception:
                radius_rsun = radius_m = luminosity_lsun = None

        batch.append((radius_rsun, radius_m, luminosity_lsun, method, temp_source, mag_source, row_id))
        if len(batch) >= 1000:
            cur.executemany(
                'UPDATE stars SET radius_rsun=?, radius_m=?, luminosity_lsun=?, radius_method=?, radius_temp_source=?, radius_mag_source=? WHERE merged_row_id=?',
                batch,
            )
            conn.commit()
            batch = []

    if batch:
        cur.executemany(
            'UPDATE stars SET radius_rsun=?, radius_m=?, luminosity_lsun=?, radius_method=?, radius_temp_source=?, radius_mag_source=? WHERE merged_row_id=?',
            batch,
        )
        conn.commit()

    stats = {
        'total_rows': cur.execute('SELECT COUNT(*) FROM stars').fetchone()[0],
        'radius_populated': cur.execute('SELECT COUNT(*) FROM stars WHERE radius_rsun IS NOT NULL').fetchone()[0],
        'radius_method_counts': cur.execute('SELECT radius_method, COUNT(*) FROM stars GROUP BY radius_method ORDER BY COUNT(*) DESC').fetchall(),
        'temp_source_counts': cur.execute('SELECT radius_temp_source, COUNT(*) FROM stars GROUP BY radius_temp_source ORDER BY COUNT(*) DESC').fetchall(),
        'mag_source_counts': cur.execute('SELECT radius_mag_source, COUNT(*) FROM stars GROUP BY radius_mag_source ORDER BY COUNT(*) DESC').fetchall(),
        'radius_range': cur.execute('SELECT MIN(radius_rsun), MAX(radius_rsun), AVG(radius_rsun) FROM stars WHERE radius_rsun IS NOT NULL').fetchone(),
        'sample': cur.execute('SELECT merged_display_name, HIP, gaia_source_id, merged_absolute_magnitude, temperature_k, radius_rsun, radius_method FROM stars WHERE radius_rsun IS NOT NULL ORDER BY merged_apparent_magnitude ASC LIMIT 15').fetchall(),
    }

    print('total_rows', stats['total_rows'])
    print('radius_populated', stats['radius_populated'])
    print('radius_range', stats['radius_range'])
    print('radius_method_counts', stats['radius_method_counts'])
    print('temp_source_counts', stats['temp_source_counts'])
    print('mag_source_counts', stats['mag_source_counts'])
    print('sample')
    for row in stats['sample']:
        print(row)

    conn.close()


if __name__ == '__main__':
    main()
