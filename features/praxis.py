import tempfile
import subprocess
import discord
from discord.ext import commands
import logging


class Praxis(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    def _eval(self, input_str):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(input_str.encode("utf-8"))
            fp.seek(0)
            try:
                cmd = subprocess.run(["praxis", fp.name], capture_output=True, timeout=10)
            except subprocess.TimeoutExpired:
                return "failed to evaluate (timed out)"
            return cmd.stdout.decode("utf-8")

    @commands.command()
    async def praxis(self, ctx, *, args):

        # Sanitise the input by stripping out code block formatters & whitespace
        args = args.replace("```", "")
        args = args.strip()

        logging.info(f"!praxis:\n{args}")
        output = self._eval(args)
        await ctx.send("```\n" + output + "\n```")


async def setup(bot):
    await bot.add_cog(Praxis(bot))
