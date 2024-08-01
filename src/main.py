# %%
# Running Imports #

import os
import random
import time

import pygame

# %%
# Variables #

# Screen dimensions
SCREEN_WIDTH = 800
SCREEN_HEIGHT = 600

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)

# Game settings
BALL_SPEED = 5
PADDLE_SPEED = 10

# frame dir is PARENT dir/data/frames/current datetime/
current_datetime = time.strftime("%Y%m%d-%H%M%S")
FRAME_DIR = os.path.join(
    os.path.dirname(os.getcwd()), "data", "frames", current_datetime
)


# %%
# Variables #

SAVE_FRAMES = False

# Screen dimensions
SCREEN_WIDTH = 800
SCREEN_HEIGHT = 600

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
RED = (255, 0, 0)

# Game settings
BALL_SPEED = 5
HOLE_WIDTH = 100

# Frame directory
CURRENT_DATETIME = time.strftime("%Y%m%d-%H%M%S")
FRAME_DIR = os.path.join(
    os.path.dirname(os.getcwd()), "data", "frames", CURRENT_DATETIME
)

# Circle settings
CIRCLE_CENTER = (SCREEN_WIDTH // 2, SCREEN_HEIGHT // 2)
CIRCLE_RADIUS = 200


# %%
# Functions #


def spawn_ball():
    while True:
        angle = random.uniform(0, 2 * 3.14159)
        x = CIRCLE_CENTER[0] + random.uniform(-CIRCLE_RADIUS, CIRCLE_RADIUS)
        y = CIRCLE_CENTER[1] + random.uniform(-CIRCLE_RADIUS, CIRCLE_RADIUS)
        if (x - CIRCLE_CENTER[0]) ** 2 + (y - CIRCLE_CENTER[1]) ** 2 < (
            CIRCLE_RADIUS - 15
        ) ** 2:
            break
    speed_x = BALL_SPEED * random.choice([-1, 1])
    speed_y = BALL_SPEED * random.choice([-1, 1])
    return pygame.Rect(x, y, 30, 30), speed_x, speed_y


def ball_within_circle(ball):
    dist_x = ball.centerx - CIRCLE_CENTER[0]
    dist_y = ball.centery - CIRCLE_CENTER[1]
    distance = (dist_x**2 + dist_y**2) ** 0.5
    return distance < CIRCLE_RADIUS - ball.width / 2


# %%
# Game #

# Initialize Pygame
pygame.init()

# Create the display
screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
pygame.display.set_caption("Ball in Circle")

# Create initial ball
balls = []
speeds = []
ball, ball_speed_x, ball_speed_y = spawn_ball()
balls.append(ball)
speeds.append((ball_speed_x, ball_speed_y))

# Create a clock object to control the frame rate
clock = pygame.time.Clock()

# Ensure the frame directory exists
if not os.path.exists(FRAME_DIR):
    os.makedirs(FRAME_DIR)

frame_count = 0

# Game loop
running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    new_balls = []
    new_speeds = []

    for ball, (ball_speed_x, ball_speed_y) in zip(balls, speeds):
        # Move the ball
        ball.x += ball_speed_x
        ball.y += ball_speed_y

        # Check for collision with the circle boundary
        dist_x = ball.centerx - CIRCLE_CENTER[0]
        dist_y = ball.centery - CIRCLE_CENTER[1]
        distance = (dist_x**2 + dist_y**2) ** 0.5

        if distance >= CIRCLE_RADIUS - ball.width / 2:
            # Calculate the reflection
            normal_x = dist_x / distance
            normal_y = dist_y / distance

            dot_product = ball_speed_x * normal_x + ball_speed_y * normal_y
            ball_speed_x -= 2 * dot_product * normal_x
            ball_speed_y -= 2 * dot_product * normal_y

            if (
                abs(ball.centerx - SCREEN_WIDTH // 2) < HOLE_WIDTH // 2
                and ball.centery < CIRCLE_CENTER[1]
            ):
                # Ball escaped through the hole
                new_ball_1, speed_x_1, speed_y_1 = spawn_ball()
                new_ball_2, speed_x_2, speed_y_2 = spawn_ball()
                new_balls.extend([new_ball_1, new_ball_2])
                new_speeds.extend([(speed_x_1, speed_y_1), (speed_x_2, speed_y_2)])
                continue

        new_balls.append(ball)
        new_speeds.append((ball_speed_x, ball_speed_y))

    balls = new_balls
    speeds = new_speeds

    # Drawing everything on the screen
    screen.fill(BLACK)
    pygame.draw.circle(screen, WHITE, CIRCLE_CENTER, CIRCLE_RADIUS, 2)
    pygame.draw.rect(
        screen,
        BLACK,
        (
            CIRCLE_CENTER[0] - HOLE_WIDTH // 2,
            CIRCLE_CENTER[1] - CIRCLE_RADIUS,
            HOLE_WIDTH,
            30,
        ),
    )

    for ball in balls:
        pygame.draw.ellipse(screen, RED, ball)

    if SAVE_FRAMES:
        # Save the current frame as an image
        frame_filename = os.path.join(FRAME_DIR, f"frame_{frame_count:04d}.png")
        pygame.image.save(screen, frame_filename)

    frame_count += 1

    # Update the display
    pygame.display.flip()
    clock.tick(60)  # Frame rate in frames per second

pygame.quit()

# %%
