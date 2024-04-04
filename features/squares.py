from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
import random
import pickle
from typing import *

import discord
from discord.ext import commands
import Paginator


GREEN_DESCRIPTION = "🟩 Green is the highest level of privileges (when the child is behaving well)."
YELLOW_DESCRIPTION = "🟨 Yellow is the next level (when the child is engaging in minor problem behaviors)."
RED_DESCRIPTION = "🟥 Red is the level on which the child is engaging in severe problem behaviors, such as a meltdown or aggressive behavior."

DESCRIPTION = GREEN_DESCRIPTION + "\n\n" + YELLOW_DESCRIPTION + "\n\n" + RED_DESCRIPTION + "\n\n"


class Color(Enum):
    GREEN = 0
    YELLOW = 1
    RED = 2

FEEDBACK = {
    Color.GREEN : GREEN_FEEDBACK,
    Color.YELLOW : YELLOW_FEEDBACK,
    Color.RED : RED_FEEDBACK
}

SQUARES = { "🟥" : Color.RED, "🟨" : Color.YELLOW, "🟩" : Color.GREEN }

IGNORED_USER_IDS = { 306917480432140301 }

@dataclass
class React:
    message_id: int
    target_id: int # The author of the message
    source_id: int # The person who reacted
    timestamp: datetime # The time of the reaction

    def _id(self):
        return (self.message_id, self_target_id, self.source_id)

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        return self._id == other._id


@dataclass
class Reacts:
    color: Color # For logging
    by_message_id: dict[int, set[React]]
    by_target_id: dict[int, set[React]]

    def __init__(self, color):
        self.color = color
        self.by_message_id = dict()
        self.by_target_id = dict()

    def add(self, react):
        logging.info(f"add {self.color} react by {react.source_id} to {react.target_id} on message({react.message_id})")
        self._add(react, react.message_id, self.by_message_id)
        self._add(react, react.target_id, self.by_target_id)

    def remove(self, react):
        logging.info(f"remove {self.color} react by {react.source_id} to {react.target_id} on message({react.message_id})")
        self._remove(react, react.message_id, self.by_message_id)
        self._remove(react, react.target_id, self.by_target_id)

    def remove_all(self, message_id):
        if message_id not in self.by_message_id:
            return
        logging.info(f"remove all {self.color} reacts on message({message_id})")
        reacts = self.by_message_id.pop(message_id)
        for react in reacts:
            self._remove(react, react.target_id, self.by_target_id)

    def _add(self, react, key, dictionary):
        if key in dictionary:
            dictionary[key].add(react)
        else:
            dictionary[key] = {react}

    def _remove(self, react, key, dictionary):
        if key in dictionary:
            dictionary[key].discard(react)
            if not dictionary[key]:
                del dictionary[key]


class Squares(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self._load_reacts()

    def _load_reacts(self):
        logging.info("load reacts")
        try:
            with open("reacts.data", 'rb') as f:
                self._reacts = pickle.load(f)
        except FileNotFoundError:
            self._reacts = {
                Color.GREEN : Reacts(Color.GREEN),
                Color.YELLOW : Reacts(Color.YELLOW),
                Color.RED : Reacts(Color.RED)
            }

    def _save_reacts(self):
        logging.info("save reacts")
        with open("reacts.data", 'wb') as f:
            pickle.dump(self._reacts, f)

    async def error(self, ctx, text):
        embed = discord.Embed(
            title="Squares",
            description=text
        )
        await ctx.send(embed=embed)

    def _tally_color(self, user_id, color):
        return len(self._reacts[color].by_target_id.get(user_id, []))

    def _tally(self, user_id):
        return { color : self._tally_color(user_id, color) for color in Color }

    def _score(self, tally):
        return tally[Color.GREEN] * 2 + tally[Color.YELLOW] * (-1) + tally[Color.RED] * (-2)

    def _user_ids(self):
        return set().union(*(set(self._reacts[color].by_target_id.keys()) for color in Color)).difference(IGNORED_USER_IDS)

    # A list of users and their tallies, ordered by decreasing score
    async def _summary(self):
        summary = [ (user, self._tally(user_id)) for user_id in self._user_ids() if (user := await self.try_fetch_user(user_id)) is not None ]
        summary.sort(key=lambda user_tally: self._score(user_tally[1]), reverse=True)
        return summary

    async def _on_reaction_upd(self, ctx, remove=False):
        color = SQUARES.get(ctx.emoji.name)
        if color is None:
            return
        channel = await self.bot.fetch_channel(ctx.channel_id)
        message = await channel.fetch_message(ctx.message_id)

        if message.author.id == ctx.user_id:
            logging.info(f"ignore {color} self-react by user({ctx.user_id}) on message({message.id})")
            return

        if await self.try_fetch_user(message.author.id) is None:
            logging.info(f"ignore {color} react on non-user({message.author.id})")
            return

        reactor = await self.try_fetch_user(ctx.user_id)

        if reactor is None:
            logging.info(f"ignore {color} react by non-user({ctx.user_id})")
            return

        if reactor.bot and reactor.id != self.bot.user.id:
            logging.info(f"ignore {color} react by bot({reactor.id})")

        discord_reaction = None
        for reaction in message.reactions:
            if reaction.emoji == ctx.emoji.name:
                discord_reaction = reaction
                break

        if remove:
            if discord_reaction is None:
                self._reacts[color].remove_all(message.id)
            else:
                found = False
                for react in self._reacts[color].by_message_id.get(message.id, []):
                    if react.source_id == ctx.user_id:
                        self._reacts[color].remove(react)
                        found = True
                        break
                if not found:
                    logging.error(f"{color} react by user({ctx.user_id}) on message({message.id}) not found")
        else:
            react = React(message.id, message.author.id, ctx.user_id, datetime.now())
            self._reacts[color].add(react)

        self._save_reacts()

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, ctx):
        await self._on_reaction_upd(ctx)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, ctx):
        await self._on_reaction_upd(ctx, remove=True)

    @commands.command()
    async def info(self, ctx):
        embed = discord.Embed(
            title="Squares",
            description=DESCRIPTION
        )
        await ctx.send(embed=embed)

    async def try_fetch_user(self, user_id):
        if user_id is None:
            return None
        try:
            return await self.bot.fetch_user(user_id)
        except discord.errors.NotFound:
            logging.debug(f"failed to fetch user({user_id})")
            return None

    @commands.command()
    async def squares(self, ctx):
        summary = await self._summary()
        if not summary:
            await self.error(ctx, "No users found.")
            return

        description = "All-time user behaviour statistics."

        ROWS_PER_PAGE=10
        embeds = []
        i = 0
        while i < len(summary):
            embed = discord.Embed(
                title="Squares",
                description=description
            )
            table = ""
            page = summary[i:i+ROWS_PER_PAGE]
            for (user, tally) in page:
                row = f"{i+1}. {user.name}: {tally[Color.GREEN]}🟩 {tally[Color.YELLOW]}🟨 {tally[Color.RED]}🟥"
                if table != "":
                    table += "\n"
                table += row
                i += 1
            embed.add_field(name="```"+table+"```", value="", inline=False)
            embeds.append(embed)

        if len(embeds) > 1:
            await Paginator.Simple().start(ctx, pages=embeds)
        else:
            assert(len(embeds) == 1)
            await ctx.send(embed=embeds[0])


async def setup(bot):
    await bot.add_cog(Squares(bot))
