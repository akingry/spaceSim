import math
import sqlite3
from pathlib import Path

import moderngl
import numpy as np
import pygame
from pygame.locals import DOUBLEBUF, FULLSCREEN, OPENGL
from pyrr import Matrix44

MERGED_DB_PATH = Path(r"D:\OC\spaceSim\hip_gaia_merged.db")
MAX_STARS = 60000
MOUSE_SENSITIVITY = 0.12
INITIAL_MAG_LIMIT = 9.0
MIN_MAG_LIMIT = -2.0
MAX_MAG_LIMIT = 14.0
VISIBLE_MAG_MIN = -1.5
VISIBLE_MAG_MAX = 8.0
POINT_SIZE_MIN = 1.0
POINT_SIZE_MAX = 4.0
ALPHA_MIN = 0.15
ALPHA_MAX = 1.0
MOVE_SPEED_LY_PER_SEC = 50.0
PARSEC_TO_LIGHTYEAR = 3.26156
KM_PER_PC = 3.085677581e13
PC_TO_M = 3.085677581491367e16
R_SUN_M = 6.957e8
R_SUN_PC = R_SUN_M / PC_TO_M
DEFAULT_MOVE_SPEED_PC = MOVE_SPEED_LY_PER_SEC / PARSEC_TO_LIGHTYEAR
MIN_MOVE_SPEED_PC = 1e-10
MAX_MOVE_SPEED_PC = 1000.0
MIN_RENDER_DISTANCE_PC = 0.05
RETICLE_SIZE = 8
HUD_COLOR = (185, 195, 220, 150)
LABEL_COLOR = (185, 195, 220, 190)
LABEL_MAX_DISTANCE_LY = 10.0
MAX_LABELS = 40
PICK_RADIUS_PX = 16
PANEL_BG = (10, 16, 28, 220)
PANEL_BORDER = (92, 120, 170, 235)
PANEL_TITLE = (235, 242, 255)
PANEL_TEXT = (205, 216, 235)
PANEL_MUTED = (135, 150, 178)
PANEL_ACCENT = (155, 205, 255)
GOTO_SPEED_LY_PER_SEC = 5.0
GOTO_SPEED_PC = GOTO_SPEED_LY_PER_SEC / PARSEC_TO_LIGHTYEAR
GOTO_STOP_DISTANCE_PC = 0.02
COMPANION_RADIUS_LY = 0.5
MAX_COMPANION_SPHERES = 8
SPHERE_MIN_RADIUS_PC = 0.0

VERTEX_SHADER = """
#version 330
uniform mat4 u_projection;
uniform mat4 u_view;
in vec3 in_position;
in vec4 in_color;
in float in_size;
out vec4 v_color;
void main() {
    gl_Position = u_projection * u_view * vec4(in_position, 1.0);
    gl_PointSize = in_size;
    v_color = in_color;
}
"""

FRAGMENT_SHADER = """
#version 330
in vec4 v_color;
out vec4 f_color;
void main() {
    vec2 c = gl_PointCoord - vec2(0.5);
    float d = dot(c, c);
    if (d > 0.25) {
        discard;
    }
    f_color = v_color;
}
"""

LINE_VERTEX_SHADER = """
#version 330
uniform mat4 u_projection;
uniform mat4 u_view;
in vec3 in_position;
in vec4 in_color;
out vec4 v_color;
void main() {
    gl_Position = u_projection * u_view * vec4(in_position, 1.0);
    v_color = in_color;
}
"""

LINE_FRAGMENT_SHADER = """
#version 330
in vec4 v_color;
out vec4 f_color;
void main() {
    f_color = v_color;
}
"""

HUD_VERTEX_SHADER = """
#version 330
in vec2 in_position;
in vec2 in_texcoord;
out vec2 v_texcoord;
void main() {
    gl_Position = vec4(in_position, 0.0, 1.0);
    v_texcoord = in_texcoord;
}
"""

HUD_FRAGMENT_SHADER = """
#version 330
uniform sampler2D u_tex;
in vec2 v_texcoord;
out vec4 f_color;
void main() {
    f_color = texture(u_tex, v_texcoord);
}
"""

SPHERE_VERTEX_SHADER = """
#version 330
uniform mat4 u_projection;
uniform mat4 u_view;
in vec3 in_position;
in vec3 in_normal;
in vec3 in_center;
in vec3 in_color;
in float in_radius;
out vec3 v_normal;
out vec3 v_color;
void main() {
    vec3 world_pos = in_center + in_position * in_radius;
    gl_Position = u_projection * u_view * vec4(world_pos, 1.0);
    v_normal = in_normal;
    v_color = in_color;
}
"""

SPHERE_FRAGMENT_SHADER = """
#version 330
uniform vec3 u_light_dir;
in vec3 v_normal;
in vec3 v_color;
out vec4 f_color;
void main() {
    vec3 n = normalize(v_normal);
    float diffuse = max(dot(n, normalize(u_light_dir)), 0.0);
    float ambient = 0.18;
    vec3 color = v_color * (ambient + 0.82 * diffuse);
    f_color = vec4(color, 1.0);
}
"""


def choose_display_name(row):
    merged = (row.get("merged_display_name") or "").strip()
    if merged:
        return merged
    common = (row.get("merged_common_name") or row.get("common_name") or "").strip()
    if common:
        return common
    hip = row.get("HIP")
    if hip:
        return f"HIP {hip}"
    gaia_name = (row.get("gaia_name") or "").strip()
    if gaia_name:
        return gaia_name
    source_id = row.get("gaia_source_id")
    return f"Source {source_id}" if source_id is not None else "Unnamed star"


def choose_subtitle(row):
    parts = []
    hip = (row.get("HIP") or "").strip()
    if hip:
        parts.append(f"HIP {hip}")
    gaia_source_id = row.get("gaia_source_id")
    if gaia_source_id is not None:
        parts.append(f"Gaia DR3 {gaia_source_id}")
    preferred = (row.get("preferred_catalog") or "").strip()
    if preferred:
        parts.append(f"prefers {preferred}")
    return "  •  ".join(parts) if parts else f"Source {row.get('merge_key', '?')}"


def load_star_data(limit=MAX_STARS):
    if not MERGED_DB_PATH.exists():
        raise RuntimeError(f"Merged DB not found: {MERGED_DB_PATH}")

    conn = sqlite3.connect(MERGED_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT *
        FROM stars
        WHERE has_valid_3d = 1
          AND x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
          AND merged_apparent_magnitude IS NOT NULL
          AND color_r IS NOT NULL AND color_g IS NOT NULL AND color_b IS NOT NULL
          AND radius_rsun IS NOT NULL
        ORDER BY merged_apparent_magnitude ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    rows = [dict(row) for row in rows]

    if not rows:
        raise RuntimeError("No valid 3D stars found in merged database")

    positions = np.array([[row["x"], row["y"], row["z"]] for row in rows], dtype=np.float32)
    home_distance_pc = np.array([row["distance_pc"] for row in rows], dtype=np.float32)
    magnitudes = np.array([row["merged_apparent_magnitude"] for row in rows], dtype=np.float32)
    base_colors = np.array([[row["color_r"], row["color_g"], row["color_b"]] for row in rows], dtype=np.float32)
    physical_radii_pc = np.array([max(float(row["radius_rsun"]) * R_SUN_PC, SPHERE_MIN_RADIUS_PC) for row in rows], dtype=np.float32)
    labels = np.array([choose_display_name(row) for row in rows], dtype=object)
    star_records = rows

    radii = np.linalg.norm(positions, axis=1)
    finite = radii[np.isfinite(radii) & (radii > 0)]
    median_r = float(np.median(finite)) if len(finite) else 1.0
    scale = 30.0 / median_r if median_r > 0 else 1.0
    positions *= scale
    home_distance_pc *= scale
    physical_radii_pc *= scale
    return np.ascontiguousarray(positions), np.ascontiguousarray(home_distance_pc), np.ascontiguousarray(magnitudes), np.ascontiguousarray(base_colors), np.ascontiguousarray(physical_radii_pc), labels, star_records, scale


def style_from_magnitude(magnitudes, base_colors):
    mags = np.clip(magnitudes, VISIBLE_MAG_MIN, VISIBLE_MAG_MAX)
    t = (VISIBLE_MAG_MAX - mags) / (VISIBLE_MAG_MAX - VISIBLE_MAG_MIN)
    t = np.clip(t, 0.0, 1.0)
    boosted = np.power(t, 0.6)
    point_sizes = POINT_SIZE_MIN + boosted * (POINT_SIZE_MAX - POINT_SIZE_MIN)
    alphas = ALPHA_MIN + boosted * (ALPHA_MAX - ALPHA_MIN)
    rgb = np.clip(base_colors, 0.0, 1.0).astype('f4')
    colors = np.column_stack([
        rgb[:, 0],
        rgb[:, 1],
        rgb[:, 2],
        alphas,
    ]).astype('f4')
    return point_sizes.astype('f4'), colors


def build_interleaved(positions, colors, sizes):
    return np.hstack([
        positions.astype('f4'),
        colors.astype('f4'),
        sizes.reshape(-1, 1).astype('f4'),
    ])


def build_instance_data(positions, colors, radii):
    return np.hstack([
        positions.astype('f4'),
        np.clip(colors, 0.0, 1.0).astype('f4'),
        radii.reshape(-1, 1).astype('f4'),
    ])


def normalize(v):
    n = np.linalg.norm(v)
    if n == 0:
        return v
    return v / n


def orientation_matrix(yaw_deg, pitch_deg):
    yaw = math.radians(yaw_deg)
    pitch = math.radians(pitch_deg)

    world_up = np.array([0.0, 1.0, 0.0], dtype='f4')
    forward = np.array([
        math.sin(yaw) * math.cos(pitch),
        math.sin(pitch),
        -math.cos(yaw) * math.cos(pitch),
    ], dtype='f4')
    forward = normalize(forward)

    right = normalize(np.cross(forward, world_up))
    if np.linalg.norm(right) < 1e-6:
        right = np.array([1.0, 0.0, 0.0], dtype='f4')
    up = normalize(np.cross(right, forward))

    view = np.array([
        [right[0], up[0], -forward[0], 0.0],
        [right[1], up[1], -forward[1], 0.0],
        [right[2], up[2], -forward[2], 0.0],
        [0.0,      0.0,   0.0,         1.0],
    ], dtype='f4')
    return view


def camera_matrix(yaw_deg, pitch_deg, observer_pos):
    view = orientation_matrix(yaw_deg, pitch_deg).copy()
    yaw = math.radians(yaw_deg)
    pitch = math.radians(pitch_deg)
    world_up = np.array([0.0, 1.0, 0.0], dtype='f4')
    forward = np.array([
        math.sin(yaw) * math.cos(pitch),
        math.sin(pitch),
        -math.cos(yaw) * math.cos(pitch),
    ], dtype='f4')
    forward = normalize(forward)
    right = normalize(np.cross(forward, world_up))
    if np.linalg.norm(right) < 1e-6:
        right = np.array([1.0, 0.0, 0.0], dtype='f4')
    up = normalize(np.cross(right, forward))
    tx = -float(np.dot(right, observer_pos))
    ty = -float(np.dot(up, observer_pos))
    tz = float(np.dot(forward, observer_pos))
    view[3, 0] = tx
    view[3, 1] = ty
    view[3, 2] = tz
    return view


def galactic_to_cartesian(lon_deg, lat_deg, radius=60.0):
    lon = math.radians(lon_deg)
    lat = math.radians(lat_deg)
    x = radius * math.cos(lat) * math.cos(lon)
    y = radius * math.sin(lat)
    z = radius * math.cos(lat) * math.sin(lon)
    return [x, y, z]


def build_guide_geometry():
    color = np.array([0.12, 0.20, 0.42, 0.28], dtype='f4')

    equator_pts = []
    equator_cols = []
    for lon in range(0, 361, 2):
        equator_pts.append(galactic_to_cartesian(lon, 0.0))
        equator_cols.append(color)
    equator_data = np.hstack([
        np.array(equator_pts, dtype='f4'),
        np.array(equator_cols, dtype='f4'),
    ])

    pole_ring_pts = []
    pole_ring_cols = []
    for lat_sign in (1.0, -1.0):
        pole_lat = 90.0 * lat_sign
        ring_lat = 84.0 * lat_sign
        prev = None
        for lon in range(0, 361, 12):
            p = galactic_to_cartesian(lon, ring_lat)
            if prev is not None:
                pole_ring_pts.extend([prev, p])
                pole_ring_cols.extend([color, color])
            prev = p
        pole = galactic_to_cartesian(0.0, pole_lat)
        pole_ring_pts.extend([
            pole,
            galactic_to_cartesian(0.0, ring_lat),
            pole,
            galactic_to_cartesian(120.0, ring_lat),
            pole,
            galactic_to_cartesian(240.0, ring_lat),
        ])
        pole_ring_cols.extend([color] * 6)

    pole_data = np.hstack([
        np.array(pole_ring_pts, dtype='f4'),
        np.array(pole_ring_cols, dtype='f4'),
    ])
    return equator_data.astype('f4'), pole_data.astype('f4')


def build_sphere_mesh(lat_steps=8, lon_steps=12):
    vertices = []
    indices = []
    for lat_idx in range(lat_steps + 1):
        theta = math.pi * lat_idx / lat_steps
        sin_theta = math.sin(theta)
        cos_theta = math.cos(theta)
        for lon_idx in range(lon_steps + 1):
            phi = 2.0 * math.pi * lon_idx / lon_steps
            sin_phi = math.sin(phi)
            cos_phi = math.cos(phi)
            x = sin_theta * cos_phi
            y = cos_theta
            z = sin_theta * sin_phi
            vertices.extend([x, y, z, x, y, z])
    row = lon_steps + 1
    for lat_idx in range(lat_steps):
        for lon_idx in range(lon_steps):
            a = lat_idx * row + lon_idx
            b = a + row
            indices.extend([a, b, a + 1, b, b + 1, a + 1])
    return np.array(vertices, dtype='f4'), np.array(indices, dtype='i4')


def build_target_group(target_idx, positions, home_distance_pc):
    if target_idx is None:
        return []
    target_pos = positions[target_idx]
    offsets = positions - target_pos
    distances_pc = np.linalg.norm(offsets, axis=1)
    radius_pc = COMPANION_RADIUS_LY / PARSEC_TO_LIGHTYEAR
    neighbor_indices = np.where((distances_pc <= radius_pc) & (distances_pc > 0))[0]
    ordered = sorted(neighbor_indices.tolist(), key=lambda idx: float(distances_pc[idx]))[:MAX_COMPANION_SPHERES]
    return [target_idx] + ordered


def render_visible_spheres(ctx, sphere_prog, sphere_vao, projection_bytes, view_bytes, instance_count):
    if instance_count <= 0:
        return
    ctx.disable(moderngl.BLEND)
    ctx.enable(moderngl.DEPTH_TEST | moderngl.CULL_FACE)
    sphere_prog['u_projection'].write(projection_bytes)
    sphere_prog['u_view'].write(view_bytes)
    sphere_prog['u_light_dir'].value = (0.35, 0.8, 0.45)
    sphere_vao.render(mode=moderngl.TRIANGLES, instances=instance_count)


def render_star_spheres(ctx, sphere_prog, sphere_vbo, sphere_ibo, projection_bytes, view_bytes, group_indices, target_idx, observer_pos, positions, base_colors, physical_radii_pc):
    if not group_indices:
        return

    target_distance = float(np.linalg.norm(positions[target_idx] - observer_pos)) if target_idx is not None else 0.0
    boost = 1.0 + 0.12 / max(target_distance, 0.02)

    centers = positions[group_indices].astype('f4')
    colors = np.clip(base_colors[group_indices], 0.0, 1.0).astype('f4')
    radii = np.maximum(physical_radii_pc[group_indices].astype('f4'), 1e-7) * boost

    instance_data = build_instance_data(centers, colors, radii)
    instance_vbo = ctx.buffer(instance_data.tobytes())
    vao = ctx.vertex_array(
        sphere_prog,
        [
            (sphere_vbo, '3f 3f', 'in_position', 'in_normal'),
            (instance_vbo, '3f 3f 1f/i', 'in_center', 'in_color', 'in_radius'),
        ],
        index_buffer=sphere_ibo,
    )
    try:
        render_visible_spheres(ctx, sphere_prog, vao, projection_bytes, view_bytes, len(group_indices))
    finally:
        vao.release()
        instance_vbo.release()


def movement_basis(yaw_deg, pitch_deg):
    yaw = math.radians(yaw_deg)
    pitch = math.radians(pitch_deg)
    world_up = np.array([0.0, 1.0, 0.0], dtype='f4')
    forward = np.array([
        math.sin(yaw) * math.cos(pitch),
        math.sin(pitch),
        -math.cos(yaw) * math.cos(pitch),
    ], dtype='f4')
    forward = normalize(forward)
    right = normalize(np.cross(forward, world_up))
    if np.linalg.norm(right) < 1e-6:
        right = np.array([1.0, 0.0, 0.0], dtype='f4')
    up = normalize(np.cross(right, forward))
    return forward, right, up


def apparent_magnitudes_from_observer(base_magnitudes, home_distance_pc, positions, observer_pos):
    rel = positions - observer_pos
    current_distance = np.linalg.norm(rel, axis=1)
    current_distance = np.maximum(current_distance, MIN_RENDER_DISTANCE_PC)
    home_distance = np.maximum(home_distance_pc, MIN_RENDER_DISTANCE_PC)
    delta_mag = 5.0 * np.log10(current_distance / home_distance)
    return base_magnitudes + delta_mag, current_distance


def format_speed(speed_pc_per_sec):
    speed_ly = speed_pc_per_sec * PARSEC_TO_LIGHTYEAR
    if speed_ly >= 0.01:
        return f"Speed: {speed_ly:,.3f} ly/s"
    speed_km = speed_pc_per_sec * KM_PER_PC
    return f"Speed: {speed_km:,.1f} km/s"


def adjust_speed(current_speed, direction):
    factor = 1.35 if direction > 0 else (1.0 / 1.35)
    new_speed = current_speed * factor
    return max(MIN_MOVE_SPEED_PC, min(MAX_MOVE_SPEED_PC, new_speed))


def build_hud_quad(x0, y0, x1, y1):
    return np.array([
        [x0, y0, 0.0, 0.0],
        [x1, y0, 1.0, 0.0],
        [x0, y1, 0.0, 1.0],
        [x1, y1, 1.0, 1.0],
    ], dtype='f4')


def make_hud_texture(ctx, surface):
    rgba = pygame.image.tostring(surface, 'RGBA', True)
    tex = ctx.texture(surface.get_size(), 4, rgba)
    tex.filter = (moderngl.LINEAR, moderngl.LINEAR)
    return tex


def world_to_screen(pos, view, projection, width, height):
    p = np.array([pos[0], pos[1], pos[2], 1.0], dtype='f4')
    clip = p @ view @ projection
    if clip[3] <= 0:
        return None
    ndc = clip[:3] / clip[3]
    if np.any(ndc < -1.0) or np.any(ndc > 1.0):
        return None
    x = int((ndc[0] * 0.5 + 0.5) * width)
    y = int((1.0 - (ndc[1] * 0.5 + 0.5)) * height)
    return x, y


def format_value(value, fmt=None, fallback='—'):
    if value is None:
        return fallback
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return fallback
    try:
        if fmt is not None:
            return fmt.format(value)
    except Exception:
        pass
    return str(value)


def estimate_temperature_label(temp_k):
    if temp_k is None:
        return 'Unknown temperature'
    if temp_k >= 30000:
        return 'Blue giant range'
    if temp_k >= 10000:
        return 'Blue-white'
    if temp_k >= 7500:
        return 'White'
    if temp_k >= 6000:
        return 'Yellow-white'
    if temp_k >= 5200:
        return 'Sunlike yellow'
    if temp_k >= 3700:
        return 'Orange'
    return 'Red dwarf/giant range'


def build_star_panel_lines(star, current_distance_pc):
    title = choose_display_name(star)
    subtitle = choose_subtitle(star)

    current_distance_ly = current_distance_pc * PARSEC_TO_LIGHTYEAR if current_distance_pc is not None else None
    sol_distance_pc = star.get('distance_pc')
    sol_distance_ly = sol_distance_pc * PARSEC_TO_LIGHTYEAR if sol_distance_pc is not None else None
    temp_k = star.get('temperature_k') or star.get('gaia_temperature_k')

    rows = [
        ('Apparent mag', format_value(star.get('merged_apparent_magnitude'), '{:.2f}')),
        ('Absolute mag', format_value(star.get('merged_absolute_magnitude') or star.get('gaia_absolute_magnitude'), '{:.2f}')),
        ('Distance from Sol', f"{format_value(sol_distance_ly, '{:.2f}')} ly" if sol_distance_ly is not None else '—'),
        ('Current distance', f"{format_value(current_distance_ly, '{:.2f}')} ly" if current_distance_ly is not None else '—'),
        ('RA / Dec', f"{format_value(star.get('ra_deg'), '{:.3f}')}° / {format_value(star.get('dec_deg'), '{:.3f}')}°"),
        ('Proper motion', f"{format_value(star.get('pmra_masyr'), '{:.2f}')} / {format_value(star.get('pmdec_masyr'), '{:.2f}')} mas/yr"),
        ('Radial velocity', f"{format_value(star.get('merged_radial_velocity_km_s') or star.get('gaia_radial_velocity_km_s'), '{:.2f}')} km/s"),
        ('Temperature', f"{format_value(temp_k, '{:,.0f}')} K  •  {estimate_temperature_label(temp_k)}"),
        ('Identifiers', f"HIP {format_value(star.get('HIP'))}   Gaia {format_value(star.get('gaia_source_id'))}"),
        ('Sources', f"hip={format_value(star.get('has_hip'))}   gaia={format_value(star.get('has_gaia'))}   {format_value(star.get('preferred_catalog'))}"),
    ]
    return title, subtitle, rows


def pick_star(screen_pos, positions, current_distance, visible_mask, view, projection, width, height):
    candidates = []
    for idx in np.where(visible_mask)[0]:
        projected = world_to_screen(positions[idx], view, projection, width, height)
        if projected is None:
            continue
        dx = projected[0] - screen_pos[0]
        dy = projected[1] - screen_pos[1]
        dist2 = dx * dx + dy * dy
        if dist2 <= PICK_RADIUS_PX * PICK_RADIUS_PX:
            candidates.append((dist2, float(current_distance[idx]), idx, projected))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def draw_star_panel(surface, title_font, body_font, small_font, star, current_distance_pc, mode_text):
    panel_w = min(430, max(330, surface.get_width() // 3))
    x = 18
    y = 18
    title, subtitle, rows = build_star_panel_lines(star, current_distance_pc)

    line_surfaces = []
    title_surface = title_font.render(title, True, PANEL_TITLE)
    subtitle_surface = small_font.render(subtitle, True, PANEL_MUTED)
    mode_surface = small_font.render(mode_text, True, PANEL_ACCENT)
    line_surfaces.append((title_surface, 0))
    line_surfaces.append((subtitle_surface, 8))
    line_surfaces.append((mode_surface, 8))

    for label, value in rows:
        label_surface = small_font.render(label.upper(), True, PANEL_MUTED)
        value_surface = body_font.render(value, True, PANEL_TEXT)
        line_surfaces.append((label_surface, 4))
        line_surfaces.append((value_surface, 14))

    content_h = 18
    for surf, gap in line_surfaces:
        content_h += surf.get_height() + gap
    panel_h = content_h + 18

    panel_rect = pygame.Rect(x, y, panel_w, panel_h)
    pygame.draw.rect(surface, PANEL_BG, panel_rect, border_radius=14)
    pygame.draw.rect(surface, PANEL_BORDER, panel_rect, width=1, border_radius=14)

    yy = y + 16
    for surf, gap in line_surfaces:
        surface.blit(surf, (x + 16, yy))
        yy += surf.get_height() + gap


def main():
    positions, home_distance_pc, magnitudes, base_colors, physical_radii_pc, labels, star_records, render_scale = load_star_data()
    apparent_magnitudes = magnitudes.copy()
    point_sizes, colors = style_from_magnitude(apparent_magnitudes, base_colors)

    pygame.init()
    pygame.display.set_caption(f"Merged Hipparcos + Gaia Viewer - mag <= {INITIAL_MAG_LIMIT:.1f}")
    info = pygame.display.Info()
    hud_font = pygame.font.SysFont('Segoe UI', 18)
    label_font = pygame.font.SysFont('Segoe UI', 14)
    panel_title_font = pygame.font.SysFont('Segoe UI Semibold', 24)
    panel_body_font = pygame.font.SysFont('Segoe UI', 18)
    panel_small_font = pygame.font.SysFont('Segoe UI', 13)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_MAJOR_VERSION, 3)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_MINOR_VERSION, 3)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_PROFILE_MASK, pygame.GL_CONTEXT_PROFILE_CORE)
    pygame.display.set_mode((info.current_w, info.current_h), FULLSCREEN | DOUBLEBUF | OPENGL)

    ctx = moderngl.create_context()
    ctx.enable(moderngl.BLEND | moderngl.PROGRAM_POINT_SIZE)
    ctx.blend_func = moderngl.SRC_ALPHA, moderngl.ONE
    ctx.clear(0.0, 0.0, 0.0, 1.0)

    prog = ctx.program(vertex_shader=VERTEX_SHADER, fragment_shader=FRAGMENT_SHADER)
    line_prog = ctx.program(vertex_shader=LINE_VERTEX_SHADER, fragment_shader=LINE_FRAGMENT_SHADER)
    hud_prog = ctx.program(vertex_shader=HUD_VERTEX_SHADER, fragment_shader=HUD_FRAGMENT_SHADER)
    sphere_prog = ctx.program(vertex_shader=SPHERE_VERTEX_SHADER, fragment_shader=SPHERE_FRAGMENT_SHADER)

    visible = magnitudes <= INITIAL_MAG_LIMIT
    current_data = build_interleaved(positions[visible], colors[visible], point_sizes[visible])
    vbo = ctx.buffer(current_data.tobytes())
    vao = ctx.vertex_array(prog, [(vbo, '3f 4f 1f', 'in_position', 'in_color', 'in_size')])

    equator_data, pole_data = build_guide_geometry()
    equator_vbo = ctx.buffer(equator_data.tobytes())
    pole_vbo = ctx.buffer(pole_data.tobytes())
    equator_vao = ctx.vertex_array(line_prog, [(equator_vbo, '3f 4f', 'in_position', 'in_color')])
    pole_vao = ctx.vertex_array(line_prog, [(pole_vbo, '3f 4f', 'in_position', 'in_color')])
    sphere_vertices, sphere_indices = build_sphere_mesh()
    sphere_vbo = ctx.buffer(sphere_vertices.tobytes())
    sphere_ibo = ctx.buffer(sphere_indices.tobytes())
    sphere_vao = ctx.vertex_array(sphere_prog, [(sphere_vbo, '3f 3f', 'in_position', 'in_normal')], index_buffer=sphere_ibo)
    sphere_index_count = len(sphere_indices)

    projection = Matrix44.perspective_projection(75.0, info.current_w / max(info.current_h, 1), 0.01, 1000.0, dtype='f4')
    proj_bytes = projection.astype('f4').tobytes()
    prog['u_projection'].write(proj_bytes)
    line_prog['u_projection'].write(proj_bytes)

    inspect_mode = False
    selected_star_idx = None
    goto_target_idx = None
    goto_group_indices = []
    goto_active = False
    show_info_panel = True

    pygame.event.set_grab(True)
    pygame.mouse.set_visible(False)
    pygame.mouse.get_rel()

    yaw = 0.0
    pitch = 0.0
    observer_pos = np.array([0.0, 0.0, 0.0], dtype='f4')
    mag_limit = INITIAL_MAG_LIMIT
    current_speed = DEFAULT_MOVE_SPEED_PC
    goto_speed_pc = GOTO_SPEED_PC
    pre_goto_speed = current_speed
    last_visible_count = int(np.count_nonzero(visible))
    current_distance = np.linalg.norm(positions - observer_pos, axis=1)
    clock = pygame.time.Clock()
    running = True

    while running:
        dt = clock.tick(120) / 1000.0
        view = camera_matrix(yaw, pitch, observer_pos)
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.MOUSEBUTTONDOWN:
                if event.button == 3:
                    inspect_mode = not inspect_mode
                    pygame.event.set_grab(not inspect_mode)
                    pygame.mouse.set_visible(inspect_mode)
                    pygame.mouse.get_rel()
                elif event.button == 1 and inspect_mode:
                    projection_np = np.array(projection, dtype='f4')
                    picked = pick_star(event.pos, positions, current_distance, visible, view, projection_np, info.current_w, info.current_h)
                    if picked is not None:
                        selected_star_idx = picked
            elif event.type == pygame.MOUSEWHEEL:
                current_speed = adjust_speed(current_speed, event.y)
            elif event.type == pygame.KEYDOWN:
                if event.key in (pygame.K_ESCAPE, pygame.K_q):
                    running = False
                elif event.key == pygame.K_LEFTBRACKET:
                    mag_limit = max(MIN_MAG_LIMIT, mag_limit - 1.0)
                    pygame.display.set_caption(f"Merged Hipparcos + Gaia Viewer - mag <= {mag_limit:.1f}")
                elif event.key == pygame.K_RIGHTBRACKET:
                    mag_limit = min(MAX_MAG_LIMIT, mag_limit + 1.0)
                    pygame.display.set_caption(f"Merged Hipparcos + Gaia Viewer - mag <= {mag_limit:.1f}")
                elif event.key == pygame.K_HOME:
                    observer_pos[:] = 0.0
                    apparent_magnitudes = magnitudes.copy()
                    current_distance = np.linalg.norm(positions - observer_pos, axis=1)
                    point_sizes, colors = style_from_magnitude(apparent_magnitudes, base_colors)
                    goto_active = False
                    goto_target_idx = None
                    goto_group_indices = []
                    current_speed = pre_goto_speed
                    last_visible_count = -1
                elif event.key == pygame.K_h:
                    show_info_panel = not show_info_panel
                elif event.key == pygame.K_g and selected_star_idx is not None:
                    goto_target_idx = selected_star_idx
                    goto_group_indices = build_target_group(goto_target_idx, positions, home_distance_pc)
                    goto_active = True
                    pre_goto_speed = current_speed
                    current_speed = goto_speed_pc

        mx, my = pygame.mouse.get_rel() if not inspect_mode else (0, 0)
        yaw = (yaw + mx * MOUSE_SENSITIVITY) % 360.0
        pitch = max(-89.9, min(89.9, pitch - my * MOUSE_SENSITIVITY))

        keys = pygame.key.get_pressed()
        forward, right, up = movement_basis(yaw, pitch)
        move = np.array([0.0, 0.0, 0.0], dtype='f4')
        if goto_active and goto_target_idx is not None:
            to_target = positions[goto_target_idx] - observer_pos
            target_distance = float(np.linalg.norm(to_target))
            if target_distance <= GOTO_STOP_DISTANCE_PC:
                goto_active = False
                current_speed = pre_goto_speed
            elif target_distance > 0:
                move = normalize(to_target)
                step = min(goto_speed_pc * dt, max(target_distance - GOTO_STOP_DISTANCE_PC, 0.0))
                observer_pos += move * step
        elif not inspect_mode:
            if keys[pygame.K_w]:
                move += forward
            if keys[pygame.K_s]:
                move -= forward
            if keys[pygame.K_d]:
                move += right
            if keys[pygame.K_a]:
                move -= right
            if keys[pygame.K_e]:
                move += up
            if keys[pygame.K_q]:
                move -= up
            if np.linalg.norm(move) > 0:
                move_dir = normalize(move)
                step = current_speed * dt
                observer_pos += move_dir * step
        if goto_active or np.linalg.norm(move) > 0:
            apparent_magnitudes, current_distance = apparent_magnitudes_from_observer(magnitudes, home_distance_pc, positions, observer_pos)
            point_sizes, colors = style_from_magnitude(apparent_magnitudes, base_colors)

        visible = apparent_magnitudes <= mag_limit
        visible_count = int(np.count_nonzero(visible))

        if visible_count != last_visible_count:
            current_data = build_interleaved(positions[visible], colors[visible], point_sizes[visible])
            vbo.orphan(size=max(current_data.nbytes, 1))
            if current_data.nbytes:
                vbo.write(current_data.tobytes())
            last_visible_count = visible_count

        view = camera_matrix(yaw, pitch, observer_pos)
        view_bytes = view.astype('f4').tobytes()
        prog['u_view'].write(view_bytes)
        line_prog['u_view'].write(orientation_matrix(yaw, pitch).astype('f4').tobytes())

        ctx.clear(0.0, 0.0, 0.0, 1.0)
        if np.linalg.norm(observer_pos) < 1e-6:
            equator_vao.render(mode=moderngl.LINE_STRIP)
            pole_vao.render(mode=moderngl.LINES)
        if last_visible_count > 0:
            vao.render(mode=moderngl.POINTS, vertices=last_visible_count)
        if goto_target_idx is not None and goto_group_indices:
            render_star_spheres(ctx, sphere_prog, sphere_vbo, sphere_ibo, proj_bytes, view_bytes, goto_group_indices, goto_target_idx, observer_pos, positions, base_colors, physical_radii_pc)

        overlay = pygame.Surface((info.current_w, info.current_h), pygame.SRCALPHA)
        cx = info.current_w // 2
        cy = info.current_h // 2
        pygame.draw.line(overlay, (180, 200, 240, 180), (cx - RETICLE_SIZE, cy), (cx + RETICLE_SIZE, cy), 1)
        pygame.draw.line(overlay, (180, 200, 240, 180), (cx, cy - RETICLE_SIZE), (cx, cy + RETICLE_SIZE), 1)
        label_mask = current_distance * PARSEC_TO_LIGHTYEAR <= LABEL_MAX_DISTANCE_LY
        label_indices = np.where(label_mask)[0]
        label_texts = []
        if len(label_indices):
            projection_np = np.array(projection, dtype='f4')
            label_candidates = []
            for idx in label_indices:
                screen_pos = world_to_screen(positions[idx], view, projection_np, info.current_w, info.current_h)
                if screen_pos is None:
                    continue
                label_candidates.append((float(current_distance[idx]), idx, screen_pos))
            label_candidates.sort(key=lambda t: t[0])
            used_rects = []
            for _, idx, (sx, sy) in label_candidates[:MAX_LABELS]:
                label_surface = label_font.render(str(labels[idx]), True, LABEL_COLOR[:3])
                label_surface.set_alpha(LABEL_COLOR[3])
                rect = label_surface.get_rect(midbottom=(sx, sy - 8))
                if rect.right < 0 or rect.left > info.current_w or rect.bottom < 0 or rect.top > info.current_h:
                    continue
                if any(rect.colliderect(r.inflate(6, 4)) for r in used_rects):
                    continue
                label_texts.append((label_surface, rect))
                used_rects.append(rect)

        if goto_active and goto_target_idx is not None:
            mode_text = f"Goto mode — cruising to {labels[goto_target_idx]} at {GOTO_SPEED_LY_PER_SEC:.0f} ly/s"
        else:
            mode_text = 'Inspect mode — right click to return to flight' if inspect_mode else 'Flight mode — right click to inspect stars'
        mode_surface = label_font.render(mode_text, True, HUD_COLOR[:3])
        mode_surface.set_alpha(HUD_COLOR[3])
        overlay.blit(mode_surface, (18, info.current_h - mode_surface.get_height() - 18))

        speed_for_hud = goto_speed_pc if goto_active else current_speed
        hud_surface = hud_font.render(format_speed(speed_for_hud), True, HUD_COLOR[:3])
        hud_surface.set_alpha(HUD_COLOR[3])
        text_x = info.current_w - hud_surface.get_width() - 18
        text_y = 14
        overlay.blit(hud_surface, (text_x, text_y))
        for surf, rect in label_texts:
            overlay.blit(surf, rect.topleft)

        if show_info_panel and selected_star_idx is not None:
            live_distance_pc = float(current_distance[selected_star_idx]) / render_scale if render_scale else float(current_distance[selected_star_idx])
            draw_star_panel(overlay, panel_title_font, panel_body_font, panel_small_font, star_records[selected_star_idx], live_distance_pc, mode_text)
            if not goto_active:
                hint_surface = panel_small_font.render('Press G to go physicalize this star', True, PANEL_ACCENT)
                overlay.blit(hint_surface, (18 + 16, 18 + 16 + panel_title_font.get_height() + panel_small_font.get_height() + 34))

        hud_tex = make_hud_texture(ctx, overlay)
        hud_tex.use(0)
        hud_prog['u_tex'].value = 0
        hud_quad = build_hud_quad(-1.0, -1.0, 1.0, 1.0)
        hud_vbo = ctx.buffer(hud_quad.tobytes())
        hud_vao = ctx.vertex_array(hud_prog, [(hud_vbo, '2f 2f', 'in_position', 'in_texcoord')])
        ctx.disable(moderngl.DEPTH_TEST)
        ctx.enable(moderngl.BLEND)
        hud_vao.render(mode=moderngl.TRIANGLE_STRIP)
        hud_vao.release()
        hud_vbo.release()
        hud_tex.release()
        pygame.display.flip()

    hud_prog.release()
    sphere_vbo.release()
    sphere_ibo.release()
    sphere_vao.release()
    sphere_prog.release()
    equator_vbo.release()
    pole_vbo.release()
    equator_vao.release()
    pole_vao.release()
    line_prog.release()
    vbo.release()
    vao.release()
    prog.release()
    pygame.quit()


if __name__ == '__main__':
    main()
