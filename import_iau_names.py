import csv
import sqlite3
from pathlib import Path

DB = Path(r'D:\OC\spaceSim\hipparcos.db')
CSV_PATH = Path(r'D:\OC\spaceSim\iau_named_stars.csv')


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    try:
        cur.execute('ALTER TABLE stars ADD COLUMN common_name TEXT')
        conn.commit()
    except sqlite3.OperationalError:
        pass

    updates = []
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            hip = (row.get('HIP') or '').strip()
            name = (row.get('Name') or '').strip()
            if not hip or not name:
                continue
            updates.append((name, hip))

    cur.executemany('UPDATE stars SET common_name = ? WHERE HIP = ?', updates)
    conn.commit()

    named_total = cur.execute("SELECT COUNT(*) FROM stars WHERE common_name IS NOT NULL AND common_name != ''").fetchone()[0]
    sample = cur.execute("SELECT HIP, common_name FROM stars WHERE common_name IS NOT NULL AND common_name != '' ORDER BY CAST(HIP AS INTEGER) LIMIT 25").fetchall()

    print('csv_rows', len(updates))
    print('named_total', named_total)
    print('sample')
    for row in sample:
        print(row)

    conn.close()


if __name__ == '__main__':
    main()
