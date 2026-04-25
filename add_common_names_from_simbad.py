import csv
import io
import sqlite3
import time
import urllib.parse
import urllib.request

DB = r'D:\OC\spaceSim\hipparcos.db'
BATCH = 250
TAP = 'https://simbad.u-strasbg.fr/simbad/sim-tap/sync'


def fetch_batch(hips):
    vals = ','.join(f"'HIP {int(h)}'" for h in hips)
    query = f"""
    select i.id as hip_id, b.main_id
    from ident as i
    join basic as b on b.oid = i.oidref
    where i.id in ({vals})
    """
    params = {
        'request': 'doQuery',
        'lang': 'adql',
        'format': 'csv',
        'query': query,
    }
    url = TAP + '?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=60) as r:
        data = r.read().decode('utf-8', 'ignore')
    rows = list(csv.DictReader(io.StringIO(data)))
    out = {}
    for row in rows:
        hip_raw = (row.get('hip_id') or '').strip()
        main_id = (row.get('main_id') or '').strip()
        if not hip_raw.startswith('HIP '):
            continue
        hip = hip_raw[4:].strip()
        if main_id.startswith('NAME '):
            out[hip] = main_id[5:].strip()
    return out


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    try:
        cur.execute('ALTER TABLE stars ADD COLUMN common_name TEXT')
        conn.commit()
    except sqlite3.OperationalError:
        pass

    hips = [row[0] for row in cur.execute("SELECT HIP FROM stars WHERE HIP IS NOT NULL AND HIP != '' ORDER BY CAST(HIP AS INTEGER)").fetchall()]

    total_named = 0
    for i in range(0, len(hips), BATCH):
        batch = hips[i:i+BATCH]
        names = fetch_batch(batch)
        if names:
            updates = [(name, hip) for hip, name in names.items()]
            cur.executemany('UPDATE stars SET common_name = ? WHERE HIP = ?', updates)
            conn.commit()
            total_named += len(updates)
        print(f'batch {i//BATCH + 1}: queried {len(batch)}, named {len(names)}, total_named {total_named}')
        time.sleep(0.4)

    stats = cur.execute("SELECT COUNT(*) FROM stars WHERE common_name IS NOT NULL AND common_name != ''").fetchone()[0]
    sample = cur.execute("SELECT HIP, common_name FROM stars WHERE common_name IS NOT NULL AND common_name != '' ORDER BY CAST(HIP AS INTEGER) LIMIT 20").fetchall()
    print('named_total', stats)
    print('sample')
    for row in sample:
        print(row)

    conn.close()


if __name__ == '__main__':
    main()
