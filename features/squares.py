import abc
import asyncio
import os
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


GREEN_DESCRIPTION = "🟩 Green is the highest level of privileges (when the child is behaving well)."
YELLOW_DESCRIPTION = "🟨 Yellow is the next level (when the child is engaging in minor problem behaviors)."
RED_DESCRIPTION = "🟥 Red is the level on which the child is engaging in severe problem behaviors, such as a meltdown or aggressive behavior."

DESCRIPTION = GREEN_DESCRIPTION + "\n\n" + YELLOW_DESCRIPTION + "\n\n" + RED_DESCRIPTION + "\n\n"

SQUAREBOARD_SCORE_THRESHOLD = 6
SQUAREBOARD_CHANNEL_NAME = "squareboard"
HIDDEN_USERS = [306917480432140301, 163425991543226368]
DATA_DIR = "data"

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

    def calculate_weighted_squares_on_user(self, target_id) -> int:
        num_by_source_id = defaultdict(int)
        for react in self.by_target_id[target_id]:
            num_by_source_id[react.source_id] += 1
        score = 0.0
        for source_id, num in num_by_source_id.items():
            score += math.sqrt(num)
            # score += num / math.sqrt(len(self.by_source_id[source_id]))
        return int(score)

    def calculate_tally_on_user(self, user_id, source_id=None):
        reacts = self.by_target_id.get(user_id, [])
        if source_id is not None:
            reacts = [ react for react in reacts if react.source_id == source_id ]
        return len(reacts)

    def __len__(self):
        return sum(len(reacts) for reacts in self.by_target_id.values())


class ReactUpdates:

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


class ReactsDB:

    def __init__(self):
        self._filename = os.path.join(DATA_DIR, "reacts.data")
        self._load()

    def commit(self, react_updates: ReactUpdates):
        for (color, react) in react_updates.adds:
            self._reacts_by_color[color].add(react)
        for (color, react) in react_updates.removes:
            self._reacts_by_color[color].remove(react)
        self._save()

    def calculate_tally_on_message(self, message_id):
        return { color : len(self._reacts_by_color[color].by_message_id.get(message_id, [])) for color in Color }

    def calculate_tally_on_user(self, user_id, source_id=None):
        return { color : self._reacts_by_color[color].calculate_tally_on_user(user_id, source_id) for color in Color }

    def calculate_unique_squarers_on_message(self, message_id):
        source_ids = { react.source_id for color in Color for react in self._reacts_by_color[color].by_message_id.get(message_id, []) }
        return len(source_ids)

    def calculate_weighted_squares_on_user(self, user_id):
        green  = self._reacts_by_color[Color.GREEN ].calculate_weighted_squares_on_user(user_id)
        yellow = self._reacts_by_color[Color.YELLOW].calculate_weighted_squares_on_user(user_id)
        red    = self._reacts_by_color[Color.RED   ].calculate_weighted_squares_on_user(user_id)
        return (+2) * green + (-1) * yellow + (-2) * red

    def __getitem__(self, color) -> Reacts:
        return self._reacts_by_color[color]

    def _load(self):
        logging.info("load reacts")
        try:
            with open(self._filename, 'rb') as f:
                self._reacts_by_color = pickle.load(f)
            logging.info("loaded reacts from file: #reacts(%d) #messages(%d) #targets(%d)",
                sum(len(self._reacts_by_color[color])               for color in Color),
                sum(len(self._reacts_by_color[color].by_message_id) for color in Color),
                sum(len(self._reacts_by_color[color].by_target_id)  for color in Color))
        except FileNotFoundError:
            self._reacts_by_color = {
                Color.GREEN  : Reacts(Color.GREEN),
                Color.YELLOW : Reacts(Color.YELLOW),
                Color.RED    : Reacts(Color.RED)
            }

    def _save(self):
        logging.info("save reacts")
        with open(self._filename, 'wb') as f:
            pickle.dump(self._reacts_by_color, f)


@dataclass
class Message:
    id: int
    channel_id: int
    author_id: int
    original_content: str

    def __init__(self, discord_message: discord.Message):
        self.id = discord_message.id
        self.channel_id = discord_message.channel.id
        self.author_id = discord_message.author.id
        self.original_content = discord_message.content

class MessagesDB:

    def __init__(self):
        self._filename = os.path.join(DATA_DIR, "messages.data")
        self._load()

    def __contains__(self, message_id):
        return message_id in self._messages_by_id

    def __getitem__(self, message_id):
        return self._messages_by_id[message_id]

    def get(self, message_id):
        return self._messages_by_id.get(message_id)

    def __setitem__(self, message_id, message):
        self._messages_by_id[message_id] = message
        self._save()

    def __delitem__(self, message_id):
        del self._messages_by_id[message_id]
        self._save()

    def _load(self):
        logging.info("load message cache")
        try:
            with open(self._filename, 'rb') as f:
                self._messages_by_id = pickle.load(f)
        except FileNotFoundError:
            self._messages_by_id = dict()

    def _save(self):
        logging.info("save message cache")
        with open(self._filename, 'wb') as f:
            pickle.dump(self._messages_by_id, f)


class MessageFormatter(metaclass=abc.ABCMeta):

    async def format_message(self, message: Message, discord_message: Optional[discord.Message]) -> discord.Embed:
        return await self._format_message(message, discord_message)

    @abc.abstractmethod
    async def _format_message(self, message: Message, discord_message: Optional[discord.Message]) -> discord.Embed:
        raise NotImplementedError()


@dataclass
class SquareboardEntry:
    squareboard_message_id: int
    tally: Dict[Color, int]


class Squareboard:

    def __init__(self, channel_name, reacts: ReactsDB, messages: MessagesDB, formatter: MessageFormatter):
        self._channel_name = channel_name
        self._filename = os.path.join(DATA_DIR, "squareboard.data" if channel_name == "squareboard" else f"squareboard-{channel_name}.data")
        self._channel = None
        self._reacts = reacts
        self._messages = messages
        self._formatter = formatter
        self._load()

    # this can't be done in init for some reason
    def _ensure_channel(self, bot):
        if self._channel is None:
            [ guild ] = bot.guilds
            [ self._channel ] = [ channel for channel in guild.text_channels if channel.name == self._channel_name ]

    async def refresh_message(self, bot, discord_message: discord.Message):

        self._ensure_channel(bot)

        message_id = discord_message.id
        tally = self._reacts.calculate_tally_on_message(message_id)
        score = self._reacts.calculate_unique_squarers_on_message(message_id)
        entry = self._entries_by_id.get(message_id)

        squareboard_message = None
        if entry is not None:
            try:
                squareboard_message = await self._channel.fetch_message(entry.squareboard_message_id)
            except discord.errors.NotFound:
                pass

        async def insert():
            logging.info("squareboard insert message(%s) tally(%s)", message_id, tally)
            message = self._messages[message_id]
            embed = await self._formatter.format_message(message, discord_message)
            squareboard_message = await self._channel.send(embed=embed)
            self._entries_by_id[message_id] = SquareboardEntry(squareboard_message.id, tally)
            self._save()

        async def delete():
            logging.info("squareboard delete message(%s) tally(%s)", message_id, tally)
            await squareboard_message.delete()
            del self._entries_by_id[message_id]
            self._save()

        async def amend():
            logging.info("squareboard amend message(%s) tally(%s)", message_id, tally)
            message = self._messages[message_id]
            embed = await self._formatter.format_message(message, discord_message)
            await squareboard_message.edit(embed=embed)
            self._entries_by_id[message_id].tally = tally
            self._save()

        if squareboard_message is None:
            if score >= SQUAREBOARD_SCORE_THRESHOLD:
                await insert()
        else:
            if score < SQUAREBOARD_SCORE_THRESHOLD:
                await delete()
            elif tally != entry.tally:
                await amend()


    def _load(self):
        logging.info("load squareboard(%s)", self._channel_name)
        try:
            with open(self._filename, 'rb') as f:
                self._entries_by_id = pickle.load(f)
        except FileNotFoundError:
            self._entries_by_id = dict()

    def _save(self):
        logging.info("save squareboard(%s)", self._channel_name)
        with open(self._filename, 'wb') as f:
            pickle.dump(self._entries_by_id, f)



class CogABCMeta(commands.CogMeta, abc.ABCMeta):
    pass

class Squares(MessageFormatter, commands.Cog, metaclass=CogABCMeta):

    def __init__(self, bot):
        self._bot = bot
        self._reacts = ReactsDB()
        self._messages = MessagesDB()
        self._squareboard = Squareboard(SQUAREBOARD_CHANNEL_NAME, self._reacts, self._messages, self)
        self._users_by_id = {}
        self._lock = asyncio.Lock()

    def _user_ids(self):
        return set().union(*(set(self._reacts[color].by_target_id.keys()) for color in Color))

    # A list of users and their tallies, ordered by decreasing score
    async def _calculate_summary(self):
        async with self._lock:
            summary = [
                (user, self._reacts.calculate_tally_on_user(user_id), self._reacts.calculate_weighted_squares_on_user(user_id))
                for user_id in self._user_ids()
                if not self._should_hide_user(user_id)
                if (user := await self._try_fetch_user(user_id)) is not None
            ]
        summary.sort(key=lambda entry: entry[2], reverse=True)
        return summary

    async def _calculate_react_updates(self, discord_message, timestamp) -> ReactUpdates:
        def source_is_valid(source):
            if discord_message.author.id == source.id:
                return False # don't count self reacts
            if source.bot and source.id != self._bot.user.id:
                return False # don't count bots (except us)
            return True
        react_updates = ReactUpdates()
        for color in Color:
            desired_source_ids = set()
            for reaction in discord_message.reactions:
                if isinstance(reaction.emoji, str) and reaction.emoji == COLOR_TO_SQUARE[color]:
                    async for source in reaction.users():
                        if source_is_valid(source):
                            desired_source_ids.add(source.id)
                    break
            current_source_ids = { react.source_id for react in self._reacts[color].by_message_id[discord_message.id] }
            for source_id in desired_source_ids:
                if source_id not in current_source_ids:
                    react = React(discord_message.id, discord_message.author.id, source_id, timestamp)
                    react_updates.add(color, react)
            for react in self._reacts[color].by_message_id[discord_message.id]:
                if react.source_id not in desired_source_ids:
                    react_updates.remove(color, react)
        return react_updates

    async def _on_reaction_upd(self, ctx):
        color = SQUARE_TO_COLOR.get(ctx.emoji.name)
        if color is None: # ignore non-square reacts
            return
        channel = await self._bot.fetch_channel(ctx.channel_id)
        discord_message = await channel.fetch_message(ctx.message_id)
        if await self._try_fetch_user(discord_message.author.id) is None:
            logging.info(f"ignore {color} react on unknown user({discord_message.author.id})")
            return
        async with self._lock:
            react_updates = await self._calculate_react_updates(discord_message, datetime.now())
            if react_updates:
                await self._commit(discord_message, react_updates)

    async def _commit(self, discord_message, react_updates):
        # 1. update react state
        self._reacts.commit(react_updates)
        # 2. update message state
        # enforce invariant: message exists in cache iff at least one square react is observed
        tally = self._reacts.calculate_tally_on_message(discord_message.id)
        if any(tally[color] for color in Color):
            if discord_message.id not in self._messages:
                self._messages[discord_message.id] = Message(discord_message)
        else:
            if discord_message.id in self._messages:
                del self._messages[discord_message.id]
        # 3. update squareboard
        if not self._should_hide_user(discord_message.author.id):
            await self._squareboard.refresh_message(self._bot, discord_message)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, ctx):
        await self._on_reaction_upd(ctx)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, ctx):
        await self._on_reaction_upd(ctx)

    @commands.hybrid_command()
    async def info(self, ctx):
        embed = discord.Embed(
            title="Squares",
            description=DESCRIPTION
        )
        await ctx.send(embed=embed)

    async def _try_fetch_user(self, user_id) -> Optional[discord.User]:
        if user_id is None:
            return None
        if user_id in self._users_by_id:
            return self._users_by_id[user_id]
        user = None
        try:
            user = await self._bot.fetch_user(user_id)
        except discord.errors.NotFound:
            logging.debug("user(%d) not found", user_id)
        self._users_by_id[user_id] = user
        return user

    async def _try_fetch_discord_message(self, message: Message) -> Optional[discord.Message]:
        try:
            channel = await self._bot.fetch_channel(message.channel_id)
            discord_message = await channel.fetch_message(message.id)
        except discord.errors.NotFound:
            discord_message = None
        return discord_message

    def _should_hide_user(self, user_id):
        return user_id in HIDDEN_USERS

    async def _top(self, ctx, color, author_filter):
        async with self._lock:
            message_ids_and_counts = [
                (message_id, len(reacts))
                for message_id, reacts in self._reacts[color].by_message_id.items()
                if len(reacts) > 0
                if (author_filter is None or next(iter(reacts)).target_id == author_filter.id)
            ]
        message_ids = map(lambda p:p[0], sorted(message_ids_and_counts, key=lambda p:p[1], reverse=True))
        MAX_ENTRIES = 10
        embeds = []
        for message_id in message_ids:
            message = self._messages.get(message_id)
            if message is None:
                continue
            if self._should_hide_user(message.author_id):
                continue
            discord_message = await self._try_fetch_discord_message(message)
            async with self._lock:
                embed = await self._format_message(message, discord_message)
            embeds.append(embed)
            if len(embeds) >= MAX_ENTRIES:
                break
        await self._send_embeds(ctx, embeds)

    @commands.hybrid_command()
    async def topred(self, ctx, author: discord.User = None):
        await ctx.defer()
        await self._top(ctx, Color.RED, author)

    @commands.hybrid_command()
    async def topyellow(self, ctx, author: discord.User = None):
        await ctx.defer()
        await self._top(ctx, Color.YELLOW, author)

    @commands.hybrid_command()
    async def topgreen(self, ctx, author: discord.User = None):
        await ctx.defer()
        await self._top(ctx, Color.GREEN, author)

    @commands.hybrid_command()
    async def squares(self, ctx):
        await ctx.defer()
        summary = await self._calculate_summary()
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
        await self._send_embeds(ctx, embeds)

    async def _send_embeds(self, ctx, embeds: list[discord.Embed]):
        if not embeds:
            embed = discord.Embed(
                title="[No results]"
            )
            await ctx.send(embed=embed)
        elif len(embeds) == 1:
            [ embed ] = embeds
            await ctx.send(embed=embed)
        else:
            await Paginator.Simple().start(ctx, pages=embeds)

    async def _format_message(self, message: Message, discord_message: Optional[discord.Message]) -> discord.Embed:
        tally = self._reacts.calculate_tally_on_message(message.id)
        match max(Color, key=lambda color: tally[color]):
            case Color.RED:
                embed_color = discord.Colour.red()
            case Color.YELLOW:
                embed_color = discord.Colour.yellow()
            case Color.GREEN:
                embed_color = discord.Colour.green()
        embed = discord.Embed(
            description = message.original_content,
            colour = embed_color
        )
        if discord_message is not None:
            author = discord_message.author
        else:
            author = await self._try_fetch_user(message.author_id)
        if author is None:
            embed.set_author(name=message.author_id)
        else:
            embed.set_author(name=author.name, icon_url=(author.avatar.url if author.avatar is not None else None))
        tally_str = ' '.join([ str(tally[color]) + " " + COLOR_TO_SQUARE[color] for color in Color if tally[color] > 0 ])
        embed.add_field(name="Squares", value=tally_str, inline=False)
        if discord_message is None:
            embed.add_field(name="Original", value=f"[Deleted]", inline=False)
        else:
            if discord_message.attachments:
                embed.set_thumbnail(url=discord_message.attachments[0].url)
            embed.add_field(name="Original", value=f"[Jump!]({discord_message.jump_url})", inline=False)
        return embed


async def setup(bot):
    squares = Squares(bot)
    await bot.add_cog(squares)
