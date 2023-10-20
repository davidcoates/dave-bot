# dave-bot
A discord bot for Dave's discord server.

# Setup

Run `dave-bot.py` with Python3.10 after installing the `requirements.txt`. The bot token must be supplied in the `DISCORD_BOT_TOKEN` environment variable.

# Features & Commands

## Squares

Tracks users whose messages receive green, yellow, and red square reacts. These squares are used a proxy for good/bad behaviour.

Commands:
- `!squares`: Show a summary of behavious statistics.
- `!topgreen`, `!topyellow`, `!topred`: Gives a message of honest feedback to the user with the highest number of reacts given for the specified color.

## Praxis Evaluation

Evaluates Praxis programs. This requires the `praxis` binary being in `PATH`.

Commands:
- `!praxis <program>`: Evaluates the supplied program. Multi-line input can be provided using a code block.
