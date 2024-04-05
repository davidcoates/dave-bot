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

SQUAREBOARD_THRESHOLD = 6

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

    def __len__(self):
        return sum(len(reacts) for reacts in self.by_target_id.values())


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

    def _user_tally_color(self, user_id, color):
        return len(self._reacts[color].by_target_id.get(user_id, []))

    def _user_tally(self, user_id):
        return { color : self._user_tally_color(user_id, color) for color in Color }

    def _user_score(self, user_tally):
        return user_tally[Color.GREEN] * 2 + user_tally[Color.YELLOW] * (-1) + user_tally[Color.RED] * (-2)

    def _user_ids(self):
        return set().union(*(set(self._reacts[color].by_target_id.keys()) for color in Color))

    # A list of users and their tallies, ordered by decreasing score
    async def _summary(self):
        summary = [ (user, self._user_tally(user_id)) for user_id in self._user_ids() if (user := await self._try_fetch_user(user_id)) is not None ]
        summary.sort(key=lambda user_and_tally: self._user_score(user_and_tally[1]), reverse=True)
        return summary

    async def _on_reaction_upd(self, ctx, remove=False):

        color = SQUARE_TO_COLOR.get(ctx.emoji.name)
        if color is None:
            return

        channel = await self._bot.fetch_channel(ctx.channel_id)
        message = await self._fetch_message(channel, ctx.message_id)

        if message.author.id == ctx.user_id:
            logging.info(f"ignore {color} self-react by user({ctx.user_id}) on message({message.id})")
            return

        if await self._try_fetch_user(message.author.id) is None:
            logging.info(f"ignore {color} react on non-user({message.author.id})")
            return

        reactor = await self._try_fetch_user(ctx.user_id)

        if reactor is None:
            logging.info(f"ignore {color} react by non-user({ctx.user_id})")
            return

        if reactor.bot and reactor.id != self._bot.user.id:
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
        await self._refresh_squareboard_for_message(message)


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

    async def _try_fetch_user(self, user_id):
        if user_id is None:
            return None
        try:
            return await self._bot.fetch_user(user_id)
        except discord.errors.NotFound:
            logging.debug(f"failed to fetch user({user_id})")
            return None

    async def _fetch_message(self, channel, message_id):
        message = await channel.fetch_message(message_id)
        if message_id not in self._messages_by_id:
            self._messages_by_id[message_id] = Message(message)
            self._save_message_cache()
        return message

    async def _fetch_cached_message(self, ctx, message_id) -> Optional[Message]:
        message = self._messages_by_id.get(message_id)
        if message is not None:
            return message
        for channel in ctx.guild.text_channels:
            try:
                message = Message(await channel.fetch_message(message_id))
                self._messages_by_id[message_id] = message
                self._save_message_cache()
                return message
            except discord.errors.NotFound:
                continue
        logging.error("message(%d) not found", message_id)
        return None

    async def _top(self, ctx, color):
        messages = [ (message_id, len(reacts)) for message_id, reacts in self._reacts[color].by_message_id.items() ]
        messages = sorted(messages, key=lambda p:p[1], reverse=True)
        embed = discord.Embed(
            title=f"Top {color.name.title()} Squared Messages"
        )
        table = ""
        i = 0
        for (message_id, react_count) in messages:
            message = await self._fetch_cached_message(ctx, message_id)
            if message is None:
                continue
            user = await self._try_fetch_user(message.author_id)
            if user is None:
                continue
            square = COLOR_TO_SQUARE[color]
            embed.add_field(name=f"{react_count} {square} {user.name}", value=f"{message.jump_url}", inline=False)
            i += 1
            if i >= 5:
                break
        await ctx.send(embed=embed)


    @commands.command()
    async def topred(self, ctx):
        await self._top(ctx, Color.RED)

    @commands.command()
    async def topyellow(self, ctx):
        await self._top(ctx, Color.YELLOW)

    @commands.command()
    async def topgreen(self, ctx):
        await self._top(ctx, Color.GREEN)

    @commands.command()
    async def squares(self, ctx):
        summary = await self._summary()
        if not summary:
            await self._error(ctx, "No users found.")
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
                if i != 0:
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


#    @commands.command()
#    async def refresh(self, ctx):
#        await self._refresh_squareboard_historical(ctx)

    def _squareboard_score(self, message_id):
        unique_squarers = { react.source_id for color in Color for react in self._reacts[color].by_message_id.get(message_id, []) }
        return len(unique_squarers)

    async def _refresh_squareboard_historical(self, ctx):

        logging.info("refreshing squareboard (this may take a while)")

        message_ids_to_refresh = set()

        # add all messages with more than the threshold number of squares
        all_message_ids = set(message_id for color in Color for message_id in self._reacts[color].by_message_id)
        message_ids_to_refresh.update(message_id for message_id in all_message_ids if self._squareboard_score(message_id) >= SQUAREBOARD_THRESHOLD)

        # add all messages already on the squareboard (we may need to amend / delete)
        message_ids_to_refresh.update(self._squareboard_entries_by_id.keys())

        for message_id in message_ids_to_refresh:
            cached_message = await self._fetch_cached_message(ctx, message_id)
            channel = await self._bot.fetch_channel(cached_message.channel_id)
            message = await self._fetch_message(channel, message_id)
            await self._refresh_squareboard_for_message(message)

        logging.info("squareboard refreshed")


    async def _refresh_squareboard_for_message(self, message):

        if self._squareboard_channel is None:
            assert len(self._bot.guilds) == 1
            guild = self._bot.guilds[0]
            [ self._squareboard_channel ] = [ channel for channel in guild.text_channels if channel.name == "squareboard" ]

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
            pass


async def setup(bot):
    await bot.add_cog(Squares(bot))
