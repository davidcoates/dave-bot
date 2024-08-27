from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
import random
import pickle
import math
from typing import *

import discord
from discord.ext import commands
import Paginator

from lib.influx import *


GREEN_DESCRIPTION = "🟩 Green is the highest level of privileges (when the child is behaving well)."
YELLOW_DESCRIPTION = "🟨 Yellow is the next level (when the child is engaging in minor problem behaviors)."
RED_DESCRIPTION = "🟥 Red is the level on which the child is engaging in severe problem behaviors, such as a meltdown or aggressive behavior."

DESCRIPTION = GREEN_DESCRIPTION + "\n\n" + YELLOW_DESCRIPTION + "\n\n" + RED_DESCRIPTION + "\n\n"

SQUAREBOARD_THRESHOLD = 6
SQUAREBOARD_CHANNEL_NAME = "squareboard"

class Color(Enum):
    GREEN = 0
    YELLOW = 1
    RED = 2

SQUARE_TO_COLOR = { "🟥" : Color.RED, "🟨" : Color.YELLOW, "🟩" : Color.GREEN }
COLOR_TO_SQUARE = { Color.RED : "🟥", Color.YELLOW : "🟨", Color.GREEN : "🟩" }

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
    by_message_id: defaultdict[int, set[React]]
    by_target_id: defaultdict[int, set[React]]
    by_source_id: defaultdict[int, set[React]]

    def __init__(self, color):
        self.color = color
        self.by_message_id = defaultdict(set)
        self.by_target_id = defaultdict(set)
        self.by_source_id = defaultdict(set)

    def add(self, react):
        logging.info(f"add {self.color} react by {react.source_id} to {react.target_id} on message({react.message_id})")
        self.by_message_id[react.message_id].add(react)
        self.by_target_id[react.target_id].add(react)
        self.by_source_id[react.source_id].add(react)

    def remove(self, react):
        logging.info(f"remove {self.color} react by {react.source_id} to {react.target_id} on message({react.message_id})")
        self.by_message_id[react.message_id].discard(react)
        self.by_target_id[react.target_id].discard(react)
        self.by_source_id[react.source_id].discard(react)

    def weighted_squares_received(self, target_id) -> int:
        num_by_source_id = defaultdict(int)
        for react in self.by_target_id[target_id]:
            num_by_source_id[react.source_id] += 1
        score = 0.0
        for source_id, num in num_by_source_id.items():
            score += math.sqrt(num)
            # score += num / math.sqrt(len(self.by_source_id[source_id]))
        return int(score)

    def __len__(self):
        return sum(len(reacts) for reacts in self.by_target_id.values())


class Transaction:

    def __init__(self):
        self.adds = []
        self.removes = []
        self.user_pairs = set()

    def add(self, color, react):
        self.adds.append((color, react))
        self.user_pairs.add((react.source_id, react.target_id))

    def remove(self, color, react):
        self.removes.append((color, react))
        self.user_pairs.add((react.source_id, react.target_id))

    def __bool__(self):
        return bool(self.adds) or bool(self.removes)


@dataclass
class Message:
    message_id: int
    channel_id: int
    author_id: int
    jump_url: str

    def __init__(self, message):
        self.message_id = message.id
        self.channel_id = message.channel.id
        self.author_id = message.author.id
        self.jump_url = message.jump_url


@dataclass
class SquareboardEntry:
    squareboard_message_id: int
    tally: Dict[Color, int]


class Squares(commands.Cog):

    def __init__(self, bot):
        self._bot = bot
        self._load_reacts()
        self._load_message_cache()
        self._load_squareboard()
        self._squareboard_channel = None
        self._users_by_id = {}
        self._influx = InfluxDBClient()

    async def warmup(self):
        logging.info("warming up...")
        for user_id in self._user_ids():
            await self._try_fetch_user(user_id)
        logging.info("warmup finished")

    def _load_reacts(self):
        logging.info("load reacts")
        try:
            with open("reacts.data", 'rb') as f:
                self._reacts = pickle.load(f)
            logging.info("loaded reacts from file: #reacts(%d) #messages(%d) #targets(%d)",
                sum(len(self._reacts[color]) for color in Color),
                sum(len(self._reacts[color].by_message_id) for color in Color),
                sum(len(self._reacts[color].by_target_id) for color in Color))
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

    def _load_message_cache(self):
        logging.info("load message cache")
        try:
            with open("messages.data", 'rb') as f:
                self._messages_by_id = pickle.load(f)
        except FileNotFoundError:
            self._messages_by_id = dict()

    def _save_message_cache(self):
        logging.info("save message cache")
        with open("messages.data", 'wb') as f:
            pickle.dump(self._messages_by_id, f)

    def _load_squareboard(self):
        logging.info("load squareboard")
        try:
            with open("squareboard.data", 'rb') as f:
                self._squareboard_entries_by_id = pickle.load(f)
        except FileNotFoundError:
            self._squareboard_entries_by_id = dict()

    def _save_squareboard(self):
        logging.info("save squareboard")
        with open("squareboard.data", 'wb') as f:
            pickle.dump(self._squareboard_entries_by_id, f)

    async def _error(self, ctx, text):
        embed = discord.Embed(
            title="Squares",
            description=text
        )
        await ctx.send(embed=embed)

    def _message_tally(self, message_id):
        return { color : len(self._reacts[color].by_message_id.get(message_id, [])) for color in Color }

    def _user_tally_color(self, user_id, color, source_id=None):
        reacts = self._reacts[color].by_target_id.get(user_id, [])
        if source_id is not None:
            reacts = [ react for react in reacts if react.source_id == source_id ]
        return len(reacts)

    def _user_tally(self, user_id, source_id=None):
        return { color : self._user_tally_color(user_id, color, source_id) for color in Color }

    def _user_score(self, user_id):
        green = self._reacts[Color.GREEN].weighted_squares_received(user_id)
        yellow = self._reacts[Color.YELLOW].weighted_squares_received(user_id)
        red = self._reacts[Color.RED].weighted_squares_received(user_id)
        return (+2) * green + (-1) * yellow + (-2) * red

    def _user_ids(self):
        return set().union(*(set(self._reacts[color].by_target_id.keys()) for color in Color))

    # A list of users and their tallies, ordered by decreasing score
    async def _summary(self):
        summary = [
            (user, self._user_tally(user_id), self._user_score(user_id))
            for user_id in self._user_ids()
            if (user := await self._try_fetch_user(user_id)) is not None
        ]
        summary.sort(key=lambda entry: entry[2], reverse=True)
        return summary

    async def _sync_message(self, message, timestamp) -> Transaction:
        def source_is_valid(source):
            if message.author.id == source.id:
                return False # don't count self reacts
            if source.bot and source.id != self._bot.user_id:
                return False # don't count bots (except us)
            return True
        transaction = Transaction()
        for color in Color:
            desired_source_ids = set()
            for reaction in message.reactions:
                if isinstance(reaction.emoji, str) and reaction.emoji == COLOR_TO_SQUARE[color]:
                    async for source in reaction.users():
                        if source_is_valid(source):
                            desired_source_ids.add(source.id)
                    break
            current_source_ids = { react.source_id for react in self._reacts[color].by_message_id[message.id] }
            for source_id in desired_source_ids:
                if source_id not in current_source_ids:
                    react = React(message.id, message.author.id, source_id, timestamp)
                    transaction.add(color, react)
            for react in self._reacts[color].by_message_id[message.id]:
                if react.source_id not in desired_source_ids:
                    transaction.remove(color, react)
        return transaction

    async def _on_reaction_upd(self, ctx, remove=False):
        color = SQUARE_TO_COLOR.get(ctx.emoji.name)
        if color is None: # ignore non-square reacts
            return
        channel = await self._bot.fetch_channel(ctx.channel_id)
        message = await self._fetch_message(channel, ctx.message_id)
        if await self._try_fetch_user(message.author.id) is None:
            logging.info(f"ignore {color} react on unknown user({message.author.id})")
            return
        transaction = await self._sync_message(message, datetime.now())
        if transaction:
            await self._commit(transaction)
            await self._refresh_squareboard_for_message(message)

    async def _commit(self, transaction):
        # hack: push to influx both before and after for differences to always be accurate
        async def push_influx():
            for (source_id, target_id) in transaction.user_pairs:
                source = await self._try_fetch_user(source_id)
                target = await self._try_fetch_user(target_id)
                assert source is not None and target is not None
                self._push_influx_react(source, target)
        await push_influx()
        for (color, react) in transaction.adds:
            self._reacts[color].add(react)
        for (color, react) in transaction.removes:
            self._reacts[color].remove(react)
        await push_influx()
        self._save_reacts()

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, ctx):
        await self._on_reaction_upd(ctx)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, ctx):
        await self._on_reaction_upd(ctx, remove=True)

    @commands.hybrid_command()
    async def info(self, ctx):
        embed = discord.Embed(
            title="Squares",
            description=DESCRIPTION
        )
        await ctx.send(embed=embed)

    async def _try_fetch_user(self, user_id):
        if user_id is None:
            return None
        if user_id in self._users_by_id:
            return self._users_by_id[user_id]
        user = None
        try:
            user = await self._bot.fetch_user(user_id)
        except discord.errors.NotFound:
            logging.debug(f"failed to fetch user({user_id})")
        self._users_by_id[user_id] = user
        return user

    async def _fetch_message(self, channel, message_id):
        message = await channel.fetch_message(message_id)
        if message_id not in self._messages_by_id:
            self._messages_by_id[message_id] = Message(message)
            self._save_message_cache()
        return message

    async def _fetch_cached_message(self, ctx, message_id) -> Optional[Message]:
        if message_id in self._messages_by_id:
            return self._messages_by_id[message_id]
        message = None
        for channel in ctx.guild.text_channels:
            try:
                message = Message(await channel.fetch_message(message_id))
                break
            except discord.errors.NotFound:
                continue
        if message is None:
            logging.error("message(%d) not found", message_id)
        self._messages_by_id[message_id] = message
        self._save_message_cache()
        return message

    async def _top(self, ctx, color, author_filter):
        messages = [
            (message_id, len(reacts))
            for message_id, reacts in self._reacts[color].by_message_id.items()
            if len(reacts) > 0
            if (author_filter is None or next(iter(reacts)).target_id == author_filter.id)
        ]
        messages = sorted(messages, key=lambda p:p[1], reverse=True)
        embed = discord.Embed(
            title=f"Top {color.name.title()} Squared Messages"
        )
        table = ""
        MAX_ENTRIES = 8 # hard limit is 25 but that's too slow
        i = 0
        for (message_id, react_count) in messages:
            message = await self._fetch_cached_message(ctx, message_id)
            if message is None:
                continue
            author = await self._try_fetch_user(message.author_id)
            if author is None:
                continue
            square = COLOR_TO_SQUARE[color]
            embed.add_field(name=f"{react_count} {square} {author.name}", value=f"{message.jump_url}", inline=False)
            i += 1
            if i >= MAX_ENTRIES:
                break
        await ctx.send(embed=embed)

    @commands.hybrid_command()
    async def topred(self, ctx, author: discord.Member = None):
        await ctx.defer()
        await self._top(ctx, Color.RED, author)

    @commands.hybrid_command()
    async def topyellow(self, ctx, author: discord.Member = None):
        await ctx.defer()
        await self._top(ctx, Color.YELLOW, author)

    @commands.hybrid_command()
    async def topgreen(self, ctx, author: discord.Member = None):
        await ctx.defer()
        await self._top(ctx, Color.GREEN, author)

    @commands.hybrid_command()
    async def squares(self, ctx):
        summary = await self._summary()
        if not summary:
            await self._error(ctx, "No users found.")
            return
        rows = [
            f"{i+1}. {user.name}: {tally[Color.GREEN]}🟩 {tally[Color.YELLOW]}🟨 {tally[Color.RED]}🟥 ({score})"
            for (i, (user, tally, score)) in enumerate(summary)
        ]
        MAX_PAGE_LENGTH = 255 - 2*len("```")
        embeds = []
        i = 0
        while i < len(rows):
            page = rows[i]
            i += 1
            while i < len(rows) and len(page) + 1 + len(rows[i]) <= MAX_PAGE_LENGTH:
                page += "\n"
                page += rows[i]
                i += 1
            assert len(page) <= MAX_PAGE_LENGTH
            embed = discord.Embed(
                title="Squares",
                description="All-time user behaviour statistics."
            )
            embed.add_field(name="```"+page+"```", value="", inline=False)
            embeds.append(embed)
        if len(embeds) > 1:
            await Paginator.Simple().start(ctx, pages=embeds)
        else:
            [ embed ] = embeds
            await ctx.send(embed=embed)

    def _format_squareboard_entry(self, message, tally):
        content = ' '.join([ str(tally[color]) + " " + COLOR_TO_SQUARE[color] for color in Color if tally[color] > 0 ])
        match max(Color, key=lambda color: tally[color]):
            case Color.RED:
                embed_color = discord.Colour.red()
            case Color.YELLOW:
                embed_color = discord.Colour.yellow()
            case Color.GREEN:
                embed_color = discord.Colour.green()
        embed = discord.Embed(
            description = message.content,
            colour = embed_color
        )
        embed.set_author(name=message.author.name, icon_url=(message.author.avatar.url if message.author.avatar is not None else None))
        embed.add_field(name="Original", value=f"[Jump!]({message.jump_url})", inline=False)
        return content, embed

    def _squareboard_score(self, message_id):
        source_ids = { react.source_id for color in Color for react in self._reacts[color].by_message_id.get(message_id, []) }
        return len(source_ids)

    async def _refresh_squareboard_for_message(self, message):
        if self._squareboard_channel is None:
            [ guild ] = self._bot.guilds
            [ self._squareboard_channel ] = [ channel for channel in guild.text_channels if channel.name == SQUAREBOARD_CHANNEL_NAME ]
        tally = self._message_tally(message.id)
        score = self._squareboard_score(message.id)
        squareboard_entry = self._squareboard_entries_by_id.get(message.id)
        upd = False
        if squareboard_entry is None:
            if score >= SQUAREBOARD_THRESHOLD:
                # insert
                logging.info("squareboard insert message(%s) tally(%s)", message.id, tally)
                (content, embed) = self._format_squareboard_entry(message, tally)
                squareboard_message = await self._squareboard_channel.send(content=content, embed=embed)
                self._squareboard_entries_by_id[message.id] = SquareboardEntry(squareboard_message.id, tally)
                upd = True
        else:
            if score < SQUAREBOARD_THRESHOLD:
                # delete
                logging.info("squareboard delete message(%s) tally(%s)", message.id, tally)
                squareboard_message = await self._fetch_message(self._squareboard_channel, squareboard_entry.squareboard_message_id)
                await squareboard_message.delete()
                del self._squareboard_entries_by_id[message.id]
                upd = True
            elif tally != squareboard_entry.tally:
                # amend
                logging.info("squareboard amend message(%s) tally(%s)", message.id, tally)
                squareboard_message = await self._fetch_message(self._squareboard_channel, squareboard_entry.squareboard_message_id)
                (content, embed) = self._format_squareboard_entry(message, tally)
                await squareboard_message.edit(content=content, embed=embed)
                self._squareboard_entries_by_id[message.id].tally = tally
                upd = True
        if upd:
            self._save_squareboard()

    def _push_influx_react(self, source, target):
        tally = self._user_tally(target.id)
        self._influx.write('squares_received', tags={
            'user_id': target.id
        }, fields={
            'user_name': target.name,
            'green': tally[Color.GREEN],
            'yellow': tally[Color.YELLOW],
            'red': tally[Color.RED]
        })
        tally = self._user_tally(target.id, source.id)
        self._influx.write('squares', tags={
            'cross_id': f"{source.id}-{target.id}",
            'source_id': source.id,
            'target_id': target.id
        }, fields={
            'source_name': source.name,
            'target_name': target.name,
            'green': tally[Color.GREEN],
            'yellow': tally[Color.YELLOW],
            'red': tally[Color.RED]
        })


async def setup(bot):
    squares = Squares(bot)
    await bot.add_cog(squares)
    await squares.warmup()
