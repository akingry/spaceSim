import sqlite3

DB = r'D:\OC\spaceSim\hipparcos.db'


def ballesteros_temperature(bv):
    return 4600.0 * ((1.0 / (0.92 * bv + 1.7)) + (1.0 / (0.92 * bv + 0.62)))


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    try:
        cur.execute('ALTER TABLE stars ADD COLUMN temperature_k REAL')
        conn.commit()
    except sqlite3.OperationalError:
        pass

    rows = cur.execute('SELECT id, bv_num FROM stars WHERE bv_num IS NOT NULL').fetchall()
    batch = []
    for row_id, bv in rows:
        try:
            temp = ballesteros_temperature(float(bv))
        except Exception:
            temp = None
        batch.append((temp, row_id))
        if len(batch) >= 1000:
            cur.executemany('UPDATE stars SET temperature_k = ? WHERE id = ?', batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany('UPDATE stars SET temperature_k = ? WHERE id = ?', batch)
        conn.commit()

    total = cur.execute('SELECT COUNT(*) FROM stars').fetchone()[0]
    populated = cur.execute('SELECT COUNT(*) FROM stars WHERE temperature_k IS NOT NULL').fetchone()[0]
    sample = cur.execute('SELECT HIP, bv_num, temperature_k FROM stars WHERE temperature_k IS NOT NULL ORDER BY id LIMIT 10').fetchall()
    minmax = cur.execute('SELECT MIN(temperature_k), MAX(temperature_k), AVG(temperature_k) FROM stars WHERE temperature_k IS NOT NULL').fetchone()

    print('total', total)
    print('temperature_populated', populated)
    print('temp_min_max_avg', minmax)
    print('sample')
    for row in sample:
        print(row)

    conn.close()


if __name__ == '__main__':
    main()
