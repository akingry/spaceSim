import math
import os
import sqlite3
from astropy.io import fits

FITS_PATH = r'D:\OC\spaceSim\I_311_hip2.dat.gz.fits'
DB_PATH = r'D:\OC\spaceSim\hipparcos.db'


def to_float(text):
    text = (text or '').strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    hdul = fits.open(FITS_PATH)
    hdu = hdul[1]
    hdr = hdu.header
    cols = [hdr[f'TTYPE{i+1}'] for i in range(hdr['TFIELDS'])]

    starts = []
    widths = []
    for i in range(len(cols)):
        start = int(hdr[f'TBCOL{i+1}']) - 1
        fmt = str(hdr[f'TFORM{i+1}'])
        width = int(fmt[1:].split('.')[0])
        starts.append(start)
        widths.append(width)

    row_len = int(hdr['NAXIS1'])
    row_count = int(hdr['NAXIS2'])
    data_start = int(hdu._data_offset)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    orig_cols = ', '.join(f'[{c}] TEXT' for c in cols)
    cur.execute(
        f'''CREATE TABLE stars (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {orig_cols},
        ra_rad_num REAL,
        dec_rad_num REAL,
        ra_deg REAL,
        dec_deg REAL,
        parallax_mas REAL,
        parallax_error_mas REAL,
        distance_pc REAL,
        pmra_masyr REAL,
        pmdec_masyr REAL,
        hpmag_num REAL,
        bv_num REAL,
        vi_num REAL,
        x REAL,
        y REAL,
        z REAL,
        parallax_over_error REAL,
        frac_parallax_error REAL,
        has_valid_3d INTEGER NOT NULL,
        excluded_from_3d INTEGER NOT NULL,
        exclusion_reason TEXT
    )'''
    )

    db_cols = cols + [
        'ra_rad_num', 'dec_rad_num', 'ra_deg', 'dec_deg',
        'parallax_mas', 'parallax_error_mas', 'distance_pc',
        'pmra_masyr', 'pmdec_masyr', 'hpmag_num', 'bv_num', 'vi_num',
        'x', 'y', 'z', 'parallax_over_error', 'frac_parallax_error',
        'has_valid_3d', 'excluded_from_3d', 'exclusion_reason'
    ]
    placeholders = ','.join(['?'] * len(db_cols))
    insert_sql = f"INSERT INTO stars ({', '.join('[' + c + ']' for c in db_cols)}) VALUES ({placeholders})"

    idx = {name: i for i, name in enumerate(cols)}
    rows = []
    excluded = 0

    with open(FITS_PATH, 'rb') as f:
        f.seek(data_start)
        for _ in range(row_count):
            line = f.read(row_len)
            if len(line) != row_len:
                raise RuntimeError('Unexpected EOF while reading table rows')

            raw = [line[starts[i]:starts[i] + widths[i]].decode('ascii', 'ignore').strip() for i in range(len(cols))]

            ra = to_float(raw[idx['RArad']])
            de = to_float(raw[idx['DErad']])
            plx = to_float(raw[idx['Plx']])
            eplx = to_float(raw[idx['e_Plx']])
            pmra = to_float(raw[idx['pmRA']])
            pmde = to_float(raw[idx['pmDE']])
            hpmag = to_float(raw[idx['Hpmag']])
            bv = to_float(raw[idx['B-V']])
            vi = to_float(raw[idx['V-I']])

            ra_deg = ra * 180 / math.pi if ra is not None else None
            dec_deg = de * 180 / math.pi if de is not None else None

            dist = None
            x = y = z = None
            poe = None
            frac = None
            valid = 0
            excluded_flag = 1
            reason = []

            if plx is None or plx <= 0:
                reason.append('nonpositive_parallax')
            if ra is None or de is None:
                reason.append('missing_position')
            if plx is not None and eplx is not None and plx > 0:
                poe = plx / eplx if eplx != 0 else None
                frac = eplx / plx
                if frac > 0.2:
                    reason.append('high_frac_parallax_error')
            elif plx is not None and plx > 0:
                reason.append('missing_parallax_error')

            if not reason:
                dist = 1000.0 / plx
                x = dist * math.cos(de) * math.cos(ra)
                y = dist * math.cos(de) * math.sin(ra)
                z = dist * math.sin(de)
                valid = 1
                excluded_flag = 0
            else:
                excluded += 1

            rows.append(raw + [
                ra, de, ra_deg, dec_deg,
                plx, eplx, dist, pmra, pmde, hpmag, bv, vi,
                x, y, z, poe, frac, valid, excluded_flag,
                ';'.join(reason) if reason else None
            ])

            if len(rows) >= 1000:
                cur.executemany(insert_sql, rows)
                conn.commit()
                rows = []

    if rows:
        cur.executemany(insert_sql, rows)
        conn.commit()

    cur.execute('CREATE INDEX idx_stars_hip ON stars(HIP)')
    cur.execute('CREATE INDEX idx_stars_valid3d ON stars(has_valid_3d)')
    cur.execute('CREATE INDEX idx_stars_mag ON stars(hpmag_num)')
    cur.execute('CREATE INDEX idx_stars_xyz ON stars(x, y, z)')
    conn.commit()

    print('db', DB_PATH)
    print('rows', row_count)
    print('excluded', excluded)
    print('kept', row_count - excluded)
    print('sample_valid_rows')
    for r in cur.execute('SELECT HIP, distance_pc, x, y, z FROM stars WHERE has_valid_3d=1 LIMIT 3'):
        print(r)

    conn.close()
    hdul.close()


if __name__ == '__main__':
    main()
