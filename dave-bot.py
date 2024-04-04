#!/usr/bin/env python3

import os
import discord
import logging
import sys
from discord.ext import commands

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

token = os.getenv('DISCORD_BOT_TOKEN')
logging.info(f"token({token})")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix='!', intents=intents)

FEATURES = ["features.squares"]

@bot.event
async def setup_hook():
    for feature in FEATURES:
        logging.info(f"load feature({feature})")
        await bot.load_extension(feature)

log_handler = logging.getLogger().handlers[0]
bot.run(token, log_handler=log_handler)
