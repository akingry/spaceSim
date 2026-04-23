import math
import sqlite3
from pathlib import Path

import moderngl
import numpy as np
import pygame
from pygame.locals import DOUBLEBUF, FULLSCREEN, OPENGL
from pyrr import Matrix44

DB_PATH = Path(r"D:\OC\spaceSim\hipparcos.db")
MAX_STARS = 60000
MOUSE_SENSITIVITY = 0.12
INITIAL_MAG_LIMIT = 5.0
MIN_MAG_LIMIT = -2.0
MAX_MAG_LIMIT = 14.0
VISIBLE_MAG_MIN = -1.5
VISIBLE_MAG_MAX = 8.0
POINT_SIZE_MIN = 1.0
POINT_SIZE_MAX = 4.0
ALPHA_MIN = 0.15
ALPHA_MAX = 1.0

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


def load_star_data(limit=MAX_STARS):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT x, y, z, hpmag_num
        FROM stars
        WHERE has_valid_3d = 1
          AND x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
          AND hpmag_num IS NOT NULL
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
    magnitudes = arr[:, 3]

    radii = np.linalg.norm(positions, axis=1)
    finite = radii[np.isfinite(radii) & (radii > 0)]
    median_r = float(np.median(finite)) if len(finite) else 1.0
    scale = 30.0 / median_r if median_r > 0 else 1.0
    positions *= scale
    return np.ascontiguousarray(positions), np.ascontiguousarray(magnitudes)


def style_from_magnitude(magnitudes):
    mags = np.clip(magnitudes, VISIBLE_MAG_MIN, VISIBLE_MAG_MAX)
    t = (VISIBLE_MAG_MAX - mags) / (VISIBLE_MAG_MAX - VISIBLE_MAG_MIN)
    t = np.clip(t, 0.0, 1.0)
    boosted = np.power(t, 0.6)
    point_sizes = POINT_SIZE_MIN + boosted * (POINT_SIZE_MAX - POINT_SIZE_MIN)
    alphas = ALPHA_MIN + boosted * (ALPHA_MAX - ALPHA_MIN)
    colors = np.column_stack([
        np.ones_like(alphas),
        np.ones_like(alphas),
        np.ones_like(alphas),
        alphas,
    ]).astype('f4')
    return point_sizes.astype('f4'), colors


def camera_matrix(yaw_deg, pitch_deg):
    yaw = math.radians(yaw_deg)
    pitch = math.radians(pitch_deg)

    cy = math.cos(yaw)
    sy = math.sin(yaw)
    cp = math.cos(pitch)
    sp = math.sin(pitch)

    yaw_mat = np.array([
        [cy, 0.0, -sy, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [sy, 0.0, cy, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ], dtype='f4')

    pitch_mat = np.array([
        [1.0, 0.0, 0.0, 0.0],
        [0.0, cp, sp, 0.0],
        [0.0, -sp, cp, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ], dtype='f4')

    return pitch_mat @ yaw_mat


def build_interleaved(positions, colors, sizes):
    return np.hstack([
        positions.astype('f4'),
        colors.astype('f4'),
        sizes.reshape(-1, 1).astype('f4'),
    ])


def main():
    positions, magnitudes = load_star_data()
    point_sizes, colors = style_from_magnitude(magnitudes)

    pygame.init()
    pygame.display.set_caption(f"Hipparcos Star Viewer - mag <= {INITIAL_MAG_LIMIT:.1f}")
    info = pygame.display.Info()
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_MAJOR_VERSION, 3)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_MINOR_VERSION, 3)
    pygame.display.gl_set_attribute(pygame.GL_CONTEXT_PROFILE_MASK, pygame.GL_CONTEXT_PROFILE_CORE)
    pygame.display.set_mode((info.current_w, info.current_h), FULLSCREEN | DOUBLEBUF | OPENGL)

    ctx = moderngl.create_context()
    ctx.enable(moderngl.BLEND | moderngl.PROGRAM_POINT_SIZE)
    ctx.blend_func = moderngl.SRC_ALPHA, moderngl.ONE
    ctx.clear(0.0, 0.0, 0.0, 1.0)

    prog = ctx.program(vertex_shader=VERTEX_SHADER, fragment_shader=FRAGMENT_SHADER)

    visible = magnitudes <= INITIAL_MAG_LIMIT
    current_data = build_interleaved(positions[visible], colors[visible], point_sizes[visible])
    vbo = ctx.buffer(current_data.tobytes())
    vao = ctx.vertex_array(
        prog,
        [(vbo, '3f 4f 1f', 'in_position', 'in_color', 'in_size')],
    )

    projection = Matrix44.perspective_projection(75.0, info.current_w / max(info.current_h, 1), 0.01, 1000.0, dtype='f4')
    prog['u_projection'].write(projection.astype('f4').tobytes())

    pygame.event.set_grab(True)
    pygame.mouse.set_visible(False)
    pygame.mouse.get_rel()

    yaw = 0.0
    pitch = 0.0
    mag_limit = INITIAL_MAG_LIMIT
    last_visible_count = int(np.count_nonzero(visible))
    clock = pygame.time.Clock()
    running = True

    while running:
        clock.tick(120)
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key in (pygame.K_ESCAPE, pygame.K_q):
                    running = False
                elif event.key == pygame.K_LEFTBRACKET:
                    mag_limit = max(MIN_MAG_LIMIT, mag_limit - 1.0)
                    pygame.display.set_caption(f"Hipparcos Star Viewer - mag <= {mag_limit:.1f}")
                elif event.key == pygame.K_RIGHTBRACKET:
                    mag_limit = min(MAX_MAG_LIMIT, mag_limit + 1.0)
                    pygame.display.set_caption(f"Hipparcos Star Viewer - mag <= {mag_limit:.1f}")

        mx, my = pygame.mouse.get_rel()
        yaw = (yaw + mx * MOUSE_SENSITIVITY) % 360.0
        pitch = (pitch + my * MOUSE_SENSITIVITY) % 360.0

        visible = magnitudes <= mag_limit
        visible_count = int(np.count_nonzero(visible))
        if visible_count != last_visible_count:
            current_data = build_interleaved(positions[visible], colors[visible], point_sizes[visible])
            vbo.orphan(size=current_data.nbytes)
            vbo.write(current_data.tobytes())
            last_visible_count = visible_count

        view = camera_matrix(yaw, pitch)
        prog['u_view'].write(view.astype('f4').tobytes())

        ctx.clear(0.0, 0.0, 0.0, 1.0)
        if last_visible_count > 0:
            vao.render(mode=moderngl.POINTS, vertices=last_visible_count)
        pygame.display.flip()

    vbo.release()
    vao.release()
    prog.release()
    pygame.quit()


if __name__ == '__main__':
    main()
