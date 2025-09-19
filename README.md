# dave-bot
A discord bot for Dave's discord server.

# Setup

1. Create environment variables in `.env`:
  - `DISCORD_BOT_TOKEN`: The discord bot token.
  - `HOST_DATA_PATH`: The directory on the host where persistent data is written.
  - `HIDDEN_USER_IDS`: A comma separated list of Discord user IDs to exclude (optional).
2. `docker compose up -d`.
