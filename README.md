# dave-bot
A discord bot for Dave's discord server.

# Setup

Run `dave-bot.py` with Python3.10 after installing the `requirements.txt`. The bot token must be supplied in the `DISCORD_BOT_TOKEN` environment variable.

# Features & Commands

## Squares

Tracks users whose messages receive green, yellow, and red square reacts. These squares are used a proxy for good/bad behaviour.

Commands:
- `!info`: A decription of the square colours.
- `!squares`: Show a summary of behavious statistics.
- `!topred`, `!topyellow`, `!topgreen`: Show the most-squared messages for the given color.
