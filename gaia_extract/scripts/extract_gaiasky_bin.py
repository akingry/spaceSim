from __future__ import annotations

import argparse
import json
import math
import re
import struct
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq


STAR_SCHEMA = pa.schema([
    ("source_file", pa.string()),
    ("record_index", pa.int64()),
    ("source_id", pa.int64()),
    ("source_id_kind", pa.string()),
    ("gaia_source_id", pa.int64()),
    ("gaia_name", pa.string()),
    ("hip_id", pa.int32()),
    ("hip_name", pa.string()),
    ("common_name", pa.string()),
    ("raw_name", pa.string()),
    ("name_count", pa.int16()),
    ("x", pa.float64()),
    ("y", pa.float64()),
    ("z", pa.float64()),
    ("distance_iu", pa.float64()),
    ("distance_pc", pa.float64()),
    ("ra_rad", pa.float64()),
    ("dec_rad", pa.float64()),
    ("ra_deg", pa.float64()),
    ("dec_deg", pa.float64()),
    ("x_eq_pc", pa.float64()),
    ("y_eq_pc", pa.float64()),
    ("z_eq_pc", pa.float64()),
    ("vx", pa.float32()),
    ("vy", pa.float32()),
    ("vz", pa.float32()),
    ("mu_alpha_mas_per_year", pa.float32()),
    ("mu_delta_mas_per_year", pa.float32()),
    ("radial_velocity_km_per_s", pa.float32()),
    ("apparent_magnitude", pa.float32()),
    ("absolute_magnitude", pa.float32()),
    ("color_packed_float", pa.float32()),
    ("color_bits_hex", pa.string()),
    ("size", pa.float32()),
    ("effective_temperature_k", pa.float32()),
    ("temperature_k", pa.float32()),
    ("color_r", pa.float32()),
    ("color_g", pa.float32()),
    ("color_b", pa.float32()),
    ("has_valid_3d", pa.bool_()),
    ("excluded_from_3d", pa.bool_()),
    ("exclusion_reason", pa.string()),
])

ALIAS_SCHEMA = pa.schema([
    ("source_file", pa.string()),
    ("record_index", pa.int64()),
    ("source_id", pa.int64()),
    ("alias", pa.string()),
    ("alias_type", pa.string()),
    ("alias_order", pa.int16()),
])

HIP_RE = re.compile(r"^HIP\s+(\d+)$", re.IGNORECASE)
GAIA_RE = re.compile(r"^Gaia(?:\s+DR\d+)?\s+(.+)$", re.IGNORECASE)
IU_TO_PC = 1.0 / 30856775.81491367


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def temperature_to_rgb(temp_k: float | None) -> tuple[float | None, float | None, float | None]:
    if temp_k is None or not math.isfinite(temp_k):
        return None, None, None
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


def derive_spatial_fields(x: float, y: float, z: float) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None, float | None, bool, str | None]:
    vals = (x, y, z)
    if not all(math.isfinite(v) for v in vals):
        return (None, None, None, None, None, None, None, False, "nonfinite_position")
    distance_iu = math.sqrt(x * x + y * y + z * z)
    if not math.isfinite(distance_iu) or distance_iu <= 0:
        return (distance_iu, None, None, None, None, None, None, False, "nonpositive_distance")
    # Gaia Sky coords are (X',Y',Z')=(Y,Z,X) relative to standard equatorial XYZ.
    x_eq_iu = z
    y_eq_iu = x
    z_eq_iu = y
    ra_rad = math.atan2(y_eq_iu, x_eq_iu)
    if ra_rad < 0:
        ra_rad += 2 * math.pi
    dec_rad = math.asin(z_eq_iu / distance_iu)
    distance_pc = distance_iu * IU_TO_PC
    x_eq_pc = x_eq_iu * IU_TO_PC
    y_eq_pc = y_eq_iu * IU_TO_PC
    z_eq_pc = z_eq_iu * IU_TO_PC
    return (distance_iu, distance_pc, ra_rad, dec_rad, x_eq_pc, y_eq_pc, z_eq_pc, True, None)


def classify_alias(alias: str, source_id: int) -> str:
    alias = alias.strip()
    if not alias:
        return "empty"
    if HIP_RE.match(alias):
        return "hip"
    if GAIA_RE.match(alias):
        return "gaia"
    if alias.casefold() == f"gaia dr3 {source_id}".casefold():
        return "gaia"
    if alias.casefold() == str(source_id).casefold():
        return "source_id"
    if any(ch.isalpha() for ch in alias):
        return "common"
    return "other"


def extract_hip(names: list[str]) -> tuple[int | None, str | None]:
    for name in names:
        m = HIP_RE.match(name.strip())
        if m:
            return int(m.group(1)), name.strip()
    return None, None


def choose_common_name(names: list[str], source_id: int) -> str | None:
    for name in names:
        alias_type = classify_alias(name, source_id)
        if alias_type == "common":
            return name.strip()
    return None


def infer_gaia_identity(source_id: int, names: list[str], hip_id: int | None) -> tuple[str, int | None, str | None]:
    for name in names:
        if GAIA_RE.match(name.strip()):
            return "gaia_named", source_id, name.strip()
    if source_id >= 10_000_000_000:
        return "gaia_dr3_like", source_id, f"Gaia DR3 {source_id}"
    if hip_id is not None and source_id == hip_id:
        return "hip_like", None, None
    return "ambiguous_small_id", None, None


def flush_chunk(rows: list[dict], out_path: Path, schema: pa.Schema) -> None:
    if not rows:
        return
    columns = {field.name: [row.get(field.name) for row in rows] for field in schema}
    table = pa.table(columns, schema=schema)
    pq.write_table(table, out_path, compression="snappy")


def parse_file(bin_path: Path, out_dir: Path, rows_per_chunk: int = 100_000) -> dict:
    data = memoryview(bin_path.read_bytes())
    pos = 0
    marker, version, count = struct.unpack_from(">iii", data, pos)
    pos += 12
    if marker != -1:
        raise ValueError(f"Unexpected marker {marker}; expected -1 for versioned binary file")
    if version != 3:
        raise ValueError(f"This exporter currently supports Gaia Sky particle binary version 3 only, got {version}")

    dataset_dir = out_dir / bin_path.stem
    stars_dir = dataset_dir / "stars"
    aliases_dir = dataset_dir / "aliases"
    stars_dir.mkdir(parents=True, exist_ok=True)
    aliases_dir.mkdir(parents=True, exist_ok=True)

    star_rows: list[dict] = []
    alias_rows: list[dict] = []
    stars_chunk_index = 0
    aliases_chunk_index = 0

    summary = {
        "source_file": str(bin_path),
        "format": "Gaia Sky particle binary",
        "version": version,
        "record_count": count,
        "stars_chunk_files": [],
        "aliases_chunk_files": [],
        "null_radial_velocity_count": 0,
        "hip_id_count": 0,
        "gaia_id_count": 0,
        "common_name_count": 0,
        "valid_3d_count": 0,
        "excluded_3d_count": 0,
    }

    for record_index in range(count):
        x, y, z = struct.unpack_from(">ddd", data, pos)
        pos += 24
        vx, vy, vz, mu_alpha, mu_delta, rad_vel, app_mag, abs_mag, color, size, t_eff = struct.unpack_from(">11f", data, pos)
        pos += 44
        source_id = struct.unpack_from(">q", data, pos)[0]
        pos += 8
        name_len = struct.unpack_from(">i", data, pos)[0]
        pos += 4
        raw_name = data[pos: pos + 2 * name_len].tobytes().decode("utf-16-be") if name_len > 0 else ""
        pos += 2 * name_len
        names = [n.strip() for n in raw_name.split("|") if n.strip()]

        hip_id, hip_name = extract_hip(names)
        common_name = choose_common_name(names, source_id)
        source_id_kind, gaia_source_id, gaia_name = infer_gaia_identity(source_id, names, hip_id)

        if hip_id is not None:
            summary["hip_id_count"] += 1
        if gaia_source_id is not None:
            summary["gaia_id_count"] += 1
        if common_name is not None:
            summary["common_name_count"] += 1
        if math.isnan(rad_vel):
            summary["null_radial_velocity_count"] += 1
            rad_vel_out = None
        else:
            rad_vel_out = rad_vel

        color_bits_hex = f"0x{struct.unpack('>I', struct.pack('>f', color))[0]:08x}"
        distance_iu, distance_pc, ra_rad, dec_rad, x_eq_pc, y_eq_pc, z_eq_pc, has_valid_3d, exclusion_reason = derive_spatial_fields(x, y, z)
        if has_valid_3d:
            summary["valid_3d_count"] += 1
        else:
            summary["excluded_3d_count"] += 1
        color_r, color_g, color_b = temperature_to_rgb(t_eff)

        star_rows.append({
            "source_file": bin_path.name,
            "record_index": record_index,
            "source_id": source_id,
            "source_id_kind": source_id_kind,
            "gaia_source_id": gaia_source_id,
            "gaia_name": gaia_name,
            "hip_id": hip_id,
            "hip_name": hip_name,
            "common_name": common_name,
            "raw_name": raw_name or None,
            "name_count": len(names),
            "x": x,
            "y": y,
            "z": z,
            "distance_iu": distance_iu,
            "distance_pc": distance_pc,
            "ra_rad": ra_rad,
            "dec_rad": dec_rad,
            "ra_deg": None if ra_rad is None else math.degrees(ra_rad),
            "dec_deg": None if dec_rad is None else math.degrees(dec_rad),
            "x_eq_pc": x_eq_pc,
            "y_eq_pc": y_eq_pc,
            "z_eq_pc": z_eq_pc,
            "vx": vx,
            "vy": vy,
            "vz": vz,
            "mu_alpha_mas_per_year": mu_alpha,
            "mu_delta_mas_per_year": mu_delta,
            "radial_velocity_km_per_s": rad_vel_out,
            "apparent_magnitude": app_mag,
            "absolute_magnitude": abs_mag,
            "color_packed_float": color,
            "color_bits_hex": color_bits_hex,
            "size": size,
            "effective_temperature_k": t_eff,
            "temperature_k": t_eff,
            "color_r": color_r,
            "color_g": color_g,
            "color_b": color_b,
            "has_valid_3d": has_valid_3d,
            "excluded_from_3d": not has_valid_3d,
            "exclusion_reason": exclusion_reason,
        })

        emitted_aliases = set()
        derived_aliases = []
        if gaia_name:
            derived_aliases.append((gaia_name, "gaia_synthesized" if source_id_kind == "gaia_dr3_like" else "gaia"))
        if hip_name:
            derived_aliases.append((hip_name, "hip"))
        for alias_order, alias in enumerate(names):
            alias_type = classify_alias(alias, source_id)
            key = (alias, alias_type)
            emitted_aliases.add(key)
            alias_rows.append({
                "source_file": bin_path.name,
                "record_index": record_index,
                "source_id": source_id,
                "alias": alias,
                "alias_type": alias_type,
                "alias_order": alias_order,
            })
        for alias, alias_type in derived_aliases:
            key = (alias, alias_type)
            if key in emitted_aliases:
                continue
            alias_rows.append({
                "source_file": bin_path.name,
                "record_index": record_index,
                "source_id": source_id,
                "alias": alias,
                "alias_type": alias_type,
                "alias_order": len(names),
            })

        if len(star_rows) >= rows_per_chunk:
            out_path = stars_dir / f"part-{stars_chunk_index:05d}.parquet"
            flush_chunk(star_rows, out_path, STAR_SCHEMA)
            summary["stars_chunk_files"].append(str(out_path))
            star_rows.clear()
            stars_chunk_index += 1

        if len(alias_rows) >= rows_per_chunk:
            out_path = aliases_dir / f"part-{aliases_chunk_index:05d}.parquet"
            flush_chunk(alias_rows, out_path, ALIAS_SCHEMA)
            summary["aliases_chunk_files"].append(str(out_path))
            alias_rows.clear()
            aliases_chunk_index += 1

    if pos != len(data):
        summary["trailing_bytes"] = len(data) - pos
    else:
        summary["trailing_bytes"] = 0

    if star_rows:
        out_path = stars_dir / f"part-{stars_chunk_index:05d}.parquet"
        flush_chunk(star_rows, out_path, STAR_SCHEMA)
        summary["stars_chunk_files"].append(str(out_path))
    if alias_rows:
        out_path = aliases_dir / f"part-{aliases_chunk_index:05d}.parquet"
        flush_chunk(alias_rows, out_path, ALIAS_SCHEMA)
        summary["aliases_chunk_files"].append(str(out_path))

    summary_path = dataset_dir / "manifest.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    sample = []
    for part in summary["stars_chunk_files"][:1]:
        table = pq.read_table(part)
        sample.extend(table.slice(0, min(5, table.num_rows)).to_pylist())
        if len(sample) >= 5:
            break
    (dataset_dir / "sample_records.json").write_text(json.dumps(sample[:5], indent=2), encoding="utf-8")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Gaia Sky particle binary v3 to chunked parquet files.")
    parser.add_argument("input", type=Path, help="Path to a particles_XXXXXX.bin file")
    parser.add_argument("output", type=Path, help="Output directory root")
    parser.add_argument("--rows-per-chunk", type=int, default=100_000, help="Rows per parquet chunk")
    args = parser.parse_args()

    summary = parse_file(args.input, args.output, rows_per_chunk=args.rows_per_chunk)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
