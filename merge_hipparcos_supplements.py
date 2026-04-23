import sqlite3
import urllib.request
from pathlib import Path

BASE = 'https://cdsarc.cds.unistra.fr/ftp/I/311/'
WORK = Path(r'D:\OC\spaceSim')
DB = WORK / 'hipparcos.db'

FILES = {
    'hip7p.dat': [
        ('HIP', 1, 6), ('Fg', 8, 12), ('dpmRA', 14, 19), ('dpmDE', 21, 26),
        ('e_dpmRA', 28, 32), ('e_dpmDE', 34, 38), ('UW', 39, 129),
    ],
    'hip9p.dat': [
        ('HIP', 1, 6), ('Fg', 8, 12), ('dpmRA', 14, 19), ('dpmDE', 21, 26),
        ('ddpmRA', 28, 33), ('ddpmDE', 35, 40), ('e_dpmRA', 42, 46), ('e_dpmDE', 48, 52),
        ('e_ddpmRA', 54, 58), ('e_ddpmDE', 60, 64), ('UW', 65, 274),
    ],
    'hipvim.dat': [
        ('HIP', 1, 6), ('Fg', 8, 12), ('upsRA', 14, 19), ('upsDE', 21, 26),
        ('e_upsRA', 28, 32), ('e_upsDE', 34, 38), ('UW', 39, 129),
    ],
}


def download(name: str) -> Path:
    path = WORK / name
    urllib.request.urlretrieve(BASE + name, path)
    return path


def parse_fixed(path: Path, spec):
    rows = []
    with path.open('r', encoding='ascii', errors='ignore') as f:
        for line in f:
            line = line.rstrip('\n').rstrip('\r')
            if not line.strip():
                continue
            row = {}
            for name, a, b in spec:
                row[name] = line[a-1:b].strip()
            rows.append(row)
    return rows


def parse_int(s):
    s = (s or '').strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def decode_sn(sn_text: str):
    n = parse_int(sn_text)
    if n is None:
        return None, None, None, None, None, None
    sol_digit = n % 10
    pec = n // 10
    sol_map = {
        1: 'stochastic',
        3: 'vim',
        5: '5-parameter',
        7: '7-parameter',
        9: '9-parameter',
    }
    solution = sol_map.get(sol_digit, 'unknown')
    return (
        sol_digit,
        solution,
        1 if (pec & 1) else 0,
        1 if (pec & 2) else 0,
        1 if (pec & 4) else 0,
        1 if (pec & 8) else 0,
    )


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for sql in [
        "ALTER TABLE stars ADD COLUMN sn_solution_digit INTEGER",
        "ALTER TABLE stars ADD COLUMN sn_solution_type TEXT",
        "ALTER TABLE stars ADD COLUMN sn_flag_double INTEGER",
        "ALTER TABLE stars ADD COLUMN sn_flag_variable INTEGER",
        "ALTER TABLE stars ADD COLUMN sn_flag_photocenter INTEGER",
        "ALTER TABLE stars ADD COLUMN sn_flag_secondary INTEGER",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    rows = cur.execute("SELECT id, Sn FROM stars").fetchall()
    batch = []
    for row_id, sn in rows:
        batch.append(decode_sn(sn) + (row_id,))
        if len(batch) >= 1000:
            cur.executemany(
                "UPDATE stars SET sn_solution_digit=?, sn_solution_type=?, sn_flag_double=?, sn_flag_variable=?, sn_flag_photocenter=?, sn_flag_secondary=? WHERE id=?",
                batch,
            )
            conn.commit()
            batch = []
    if batch:
        cur.executemany(
            "UPDATE stars SET sn_solution_digit=?, sn_solution_type=?, sn_flag_double=?, sn_flag_variable=?, sn_flag_photocenter=?, sn_flag_secondary=? WHERE id=?",
            batch,
        )
        conn.commit()

    cur.execute("DROP TABLE IF EXISTS hip7p")
    cur.execute("DROP TABLE IF EXISTS hip9p")
    cur.execute("DROP TABLE IF EXISTS hipvim")

    cur.execute("CREATE TABLE hip7p (HIP TEXT PRIMARY KEY, Fg REAL, dpmRA REAL, dpmDE REAL, e_dpmRA REAL, e_dpmDE REAL, UW TEXT)")
    cur.execute("CREATE TABLE hip9p (HIP TEXT PRIMARY KEY, Fg REAL, dpmRA REAL, dpmDE REAL, ddpmRA REAL, ddpmDE REAL, e_dpmRA REAL, e_dpmDE REAL, e_ddpmRA REAL, e_ddpmDE REAL, UW TEXT)")
    cur.execute("CREATE TABLE hipvim (HIP TEXT PRIMARY KEY, Fg REAL, upsRA REAL, upsDE REAL, e_upsRA REAL, e_upsDE REAL, UW TEXT)")

    for fname, spec in FILES.items():
        path = download(fname)
        parsed = parse_fixed(path, spec)
        if fname == 'hip7p.dat':
            rows = [(r['HIP'], float(r['Fg']), float(r['dpmRA']), float(r['dpmDE']), float(r['e_dpmRA']), float(r['e_dpmDE']), r['UW']) for r in parsed]
            cur.executemany("INSERT INTO hip7p VALUES (?,?,?,?,?,?,?)", rows)
        elif fname == 'hip9p.dat':
            rows = [(r['HIP'], float(r['Fg']), float(r['dpmRA']), float(r['dpmDE']), float(r['ddpmRA']), float(r['ddpmDE']), float(r['e_dpmRA']), float(r['e_dpmDE']), float(r['e_ddpmRA']), float(r['e_ddpmDE']), r['UW']) for r in parsed]
            cur.executemany("INSERT INTO hip9p VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        elif fname == 'hipvim.dat':
            rows = [(r['HIP'], float(r['Fg']), float(r['upsRA']), float(r['upsDE']), float(r['e_upsRA']), float(r['e_upsDE']), r['UW']) for r in parsed]
            cur.executemany("INSERT INTO hipvim VALUES (?,?,?,?,?,?,?)", rows)
        conn.commit()

    cur.execute("DROP VIEW IF EXISTS stars_expanded")
    cur.execute(
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
    conn.commit()

    print('stars', cur.execute('SELECT COUNT(*) FROM stars').fetchone()[0])
    print('hip7p', cur.execute('SELECT COUNT(*) FROM hip7p').fetchone()[0])
    print('hip9p', cur.execute('SELECT COUNT(*) FROM hip9p').fetchone()[0])
    print('hipvim', cur.execute('SELECT COUNT(*) FROM hipvim').fetchone()[0])
    print('solution_counts', cur.execute('SELECT sn_solution_type, COUNT(*) FROM stars GROUP BY sn_solution_type ORDER BY sn_solution_type').fetchall())
    print('joined_7p', cur.execute("SELECT COUNT(*) FROM stars_expanded WHERE h7_Fg IS NOT NULL").fetchone()[0])
    print('joined_9p', cur.execute("SELECT COUNT(*) FROM stars_expanded WHERE h9_Fg IS NOT NULL").fetchone()[0])
    print('joined_vim', cur.execute("SELECT COUNT(*) FROM stars_expanded WHERE hv_Fg IS NOT NULL").fetchone()[0])

    conn.close()


if __name__ == '__main__':
    main()
