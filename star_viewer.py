import math
import sqlite3
from pathlib import Path

import moderngl
import numpy as np
import pygame
from pygame.locals import DOUBLEBUF, FULLSCREEN, OPENGL
from pyrr import Matrix44, Vector3

DB_PATH = Path(r"D:\OC\spaceSim\hipparcos.db")
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
DEFAULT_MOVE_SPEED_PC = MOVE_SPEED_LY_PER_SEC / PARSEC_TO_LIGHTYEAR
MIN_MOVE_SPEED_PC = 1e-10
MAX_MOVE_SPEED_PC = 1000.0
MIN_RENDER_DISTANCE_PC = 0.05
RETICLE_SIZE = 8
HUD_COLOR = (185, 195, 220, 150)

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



def load_star_data(limit=MAX_STARS):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT x, y, z, distance_pc, hpmag_num, color_r, color_g, color_b
        FROM stars
        WHERE has_valid_3d = 1
          AND x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
          AND hpmag_num IS NOT NULL
          AND color_r IS NOT NULL AND color_g IS NOT NULL AND color_b IS NOT NULL
        ORDER BY hpmag_num ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    arr = np.array(rows, dtype=np.float32)
    if len(arr) == 0:
        raise RuntimeError("No valid 3D stars found in database")

    positions = arr[:, :3]
    home_distance_pc = arr[:, 3]
    magnitudes = arr[:, 4]
    base_colors = arr[:, 5:8]

    radii = np.linalg.norm(positions, axis=1)
    finite = radii[np.isfinite(radii) & (radii > 0)]
    median_r = float(np.median(finite)) if len(finite) else 1.0
    scale = 30.0 / median_r if median_r > 0 else 1.0
    positions *= scale
    home_distance_pc *= scale
    return np.ascontiguousarray(positions), np.ascontiguousarray(home_distance_pc), np.ascontiguousarray(magnitudes), np.ascontiguousarray(base_colors)


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


def build_interleaved(positions, colors, sizes):
    return np.hstack([
        positions.astype('f4'),
        colors.astype('f4'),
        sizes.reshape(-1, 1).astype('f4'),
    ])


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
        first = None
        for lon in range(0, 361, 12):
            p = galactic_to_cartesian(lon, ring_lat)
            if first is None:
                first = p
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


def approach_target_index(positions, observer_pos, yaw_deg, pitch_deg):
    yaw = math.radians(yaw_deg)
    pitch = math.radians(pitch_deg)
    forward = np.array([
        math.sin(yaw) * math.cos(pitch),
        math.sin(pitch),
        -math.cos(yaw) * math.cos(pitch),
    ], dtype='f4')
    forward = normalize(forward)

    rel = positions - observer_pos
    distances = np.linalg.norm(rel, axis=1)
    safe_dist = np.maximum(distances, MIN_RENDER_DISTANCE_PC)
    dirs = rel / safe_dist[:, None]
    alignment = dirs @ forward

    mask = alignment > 0.98
    if np.any(mask):
        candidate_idx = np.where(mask)[0]
        score = alignment[mask] / safe_dist[mask]
        return int(candidate_idx[np.argmax(score)])

    score = alignment / safe_dist
    return int(np.argmax(score))


def main():
    positions, home_distance_pc, magnitudes, base_colors = load_star_data()
    apparent_magnitudes = magnitudes.copy()
    point_sizes, colors = style_from_magnitude(apparent_magnitudes, base_colors)

    pygame.init()
    pygame.display.set_caption(f"Hipparcos Star Viewer - mag <= {INITIAL_MAG_LIMIT:.1f}")
    info = pygame.display.Info()
    hud_font = pygame.font.SysFont('Segoe UI', 18)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_MAJOR_VERSION, 3)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_MINOR_VERSION, 3)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_PROFILE_MASK, pygame.GL_CONTEXT_PROFILE_CORE)
    screen = pygame.display.set_mode((info.current_w, info.current_h), FULLSCREEN | DOUBLEBUF | OPENGL)

    ctx = moderngl.create_context()
    ctx.enable(moderngl.BLEND | moderngl.PROGRAM_POINT_SIZE)
    ctx.blend_func = moderngl.SRC_ALPHA, moderngl.ONE
    ctx.clear(0.0, 0.0, 0.0, 1.0)

    prog = ctx.program(vertex_shader=VERTEX_SHADER, fragment_shader=FRAGMENT_SHADER)
    line_prog = ctx.program(vertex_shader=LINE_VERTEX_SHADER, fragment_shader=LINE_FRAGMENT_SHADER)
    hud_prog = ctx.program(vertex_shader=HUD_VERTEX_SHADER, fragment_shader=HUD_FRAGMENT_SHADER)

    visible = magnitudes <= INITIAL_MAG_LIMIT
    current_data = build_interleaved(positions[visible], colors[visible], point_sizes[visible])
    vbo = ctx.buffer(current_data.tobytes())
    vao = ctx.vertex_array(
        prog,
        [(vbo, '3f 4f 1f', 'in_position', 'in_color', 'in_size')],
    )

    equator_data, pole_data = build_guide_geometry()
    equator_vbo = ctx.buffer(equator_data.tobytes())
    pole_vbo = ctx.buffer(pole_data.tobytes())
    equator_vao = ctx.vertex_array(
        line_prog,
        [(equator_vbo, '3f 4f', 'in_position', 'in_color')],
    )
    pole_vao = ctx.vertex_array(
        line_prog,
        [(pole_vbo, '3f 4f', 'in_position', 'in_color')],
    )

    projection = Matrix44.perspective_projection(75.0, info.current_w / max(info.current_h, 1), 0.01, 1000.0, dtype='f4')
    proj_bytes = projection.astype('f4').tobytes()
    prog['u_projection'].write(proj_bytes)
    line_prog['u_projection'].write(proj_bytes)

    pygame.event.set_grab(True)
    pygame.mouse.set_visible(False)
    pygame.mouse.get_rel()

    yaw = 0.0
    pitch = 0.0
    observer_pos = np.array([0.0, 0.0, 0.0], dtype='f4')
    mag_limit = INITIAL_MAG_LIMIT
    current_speed = DEFAULT_MOVE_SPEED_PC
    last_visible_count = int(np.count_nonzero(visible))
    current_distance = np.linalg.norm(positions - observer_pos, axis=1)
    clock = pygame.time.Clock()
    running = True

    while running:
        dt = clock.tick(120) / 1000.0
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.MOUSEWHEEL:
                current_speed = adjust_speed(current_speed, event.y)
            elif event.type == pygame.KEYDOWN:
                if event.key in (pygame.K_ESCAPE, pygame.K_q):
                    running = False
                elif event.key == pygame.K_LEFTBRACKET:
                    mag_limit = max(MIN_MAG_LIMIT, mag_limit - 1.0)
                    pygame.display.set_caption(f"Hipparcos Star Viewer - mag <= {mag_limit:.1f}")
                elif event.key == pygame.K_RIGHTBRACKET:
                    mag_limit = min(MAX_MAG_LIMIT, mag_limit + 1.0)
                    pygame.display.set_caption(f"Hipparcos Star Viewer - mag <= {mag_limit:.1f}")
                elif event.key == pygame.K_HOME:
                    observer_pos[:] = 0.0
                    apparent_magnitudes = magnitudes.copy()
                    current_distance = np.linalg.norm(positions - observer_pos, axis=1)
                    point_sizes, colors = style_from_magnitude(apparent_magnitudes, base_colors)
                    last_visible_count = -1

        mx, my = pygame.mouse.get_rel()
        yaw = (yaw + mx * MOUSE_SENSITIVITY) % 360.0
        pitch = max(-89.9, min(89.9, pitch - my * MOUSE_SENSITIVITY))

        keys = pygame.key.get_pressed()
        forward, right, up = movement_basis(yaw, pitch)
        move = np.array([0.0, 0.0, 0.0], dtype='f4')
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
        target_idx = approach_target_index(positions, observer_pos, yaw, pitch)
        target_distance = float(current_distance[target_idx])
        if np.linalg.norm(move) > 0:
            move_dir = normalize(move)
            step = current_speed * dt
            observer_pos += move_dir * step
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

        overlay = pygame.Surface((info.current_w, info.current_h), pygame.SRCALPHA)
        cx = info.current_w // 2
        cy = info.current_h // 2
        pygame.draw.line(overlay, (180, 200, 240, 180), (cx - RETICLE_SIZE, cy), (cx + RETICLE_SIZE, cy), 1)
        pygame.draw.line(overlay, (180, 200, 240, 180), (cx, cy - RETICLE_SIZE), (cx, cy + RETICLE_SIZE), 1)
        hud_surface = hud_font.render(format_speed(current_speed), True, HUD_COLOR[:3])
        hud_surface.set_alpha(HUD_COLOR[3])
        text_x = info.current_w - hud_surface.get_width() - 18
        text_y = 14
        overlay.blit(hud_surface, (text_x, text_y))

        hud_tex = make_hud_texture(ctx, overlay)
        hud_tex.use(0)
        hud_prog['u_tex'].value = 0
        hud_quad = build_hud_quad(-1.0, -1.0, 1.0, 1.0)
        hud_vbo = ctx.buffer(hud_quad.tobytes())
        hud_vao = ctx.vertex_array(
            hud_prog,
            [(hud_vbo, '2f 2f', 'in_position', 'in_texcoord')],
        )
        ctx.disable(moderngl.DEPTH_TEST)
        ctx.enable(moderngl.BLEND)
        hud_vao.render(mode=moderngl.TRIANGLE_STRIP)
        hud_vao.release()
        hud_vbo.release()
        hud_tex.release()
        pygame.display.flip()

    hud_prog.release()
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
