import math
import sqlite3

DB = r'D:\OC\spaceSim\hipparcos.db'


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def temperature_to_rgb(temp_k):
    t = clamp(float(temp_k), 1000.0, 40000.0) / 100.0

    if t <= 66.0:
        r = 255.0
        g = 99.4708025861 * math.log(t) - 161.1195681661
        if t <= 19.0:
            b = 0.0
        else:
            b = 138.5177312231 * math.log(t - 10.0) - 305.0447927307
    else:
        r = 329.698727446 * ((t - 60.0) ** -0.1332047592)
        g = 288.1221695283 * ((t - 60.0) ** -0.0755148492)
        b = 255.0

    r = clamp(r, 0.0, 255.0)
    g = clamp(g, 0.0, 255.0)
    b = clamp(b, 0.0, 255.0)

    return r / 255.0, g / 255.0, b / 255.0


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for sql in [
        'ALTER TABLE stars ADD COLUMN color_r REAL',
        'ALTER TABLE stars ADD COLUMN color_g REAL',
        'ALTER TABLE stars ADD COLUMN color_b REAL',
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    rows = cur.execute('SELECT id, temperature_k FROM stars WHERE temperature_k IS NOT NULL').fetchall()
    batch = []
    for row_id, temp_k in rows:
        try:
            r, g, b = temperature_to_rgb(float(temp_k))
        except Exception:
            r = g = b = None
        batch.append((r, g, b, row_id))
        if len(batch) >= 1000:
            cur.executemany('UPDATE stars SET color_r = ?, color_g = ?, color_b = ? WHERE id = ?', batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany('UPDATE stars SET color_r = ?, color_g = ?, color_b = ? WHERE id = ?', batch)
        conn.commit()

    total = cur.execute('SELECT COUNT(*) FROM stars').fetchone()[0]
    populated = cur.execute('SELECT COUNT(*) FROM stars WHERE color_r IS NOT NULL AND color_g IS NOT NULL AND color_b IS NOT NULL').fetchone()[0]
    mins_maxes = cur.execute('SELECT MIN(color_r), MAX(color_r), MIN(color_g), MAX(color_g), MIN(color_b), MAX(color_b) FROM stars WHERE color_r IS NOT NULL').fetchone()
    sample = cur.execute('SELECT HIP, temperature_k, color_r, color_g, color_b FROM stars WHERE color_r IS NOT NULL ORDER BY id LIMIT 10').fetchall()

    print('total', total)
    print('rgb_populated', populated)
    print('rgb_min_max', mins_maxes)
    print('sample')
    for row in sample:
        print(row)

    conn.close()


if __name__ == '__main__':
    main()
