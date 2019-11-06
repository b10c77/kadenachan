import discord
import asyncio
from geoip import geolite2
import socket
import requests
import subprocess
from subprocess import Popen, PIPE
import json
import requests
from tabulate import tabulate

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from collections import namedtuple


import sys
KADENA_CHAN_TOKEN = sys.argv[1]
PACT_TOOL = './pact'

async def get_peer_list(loop):
    try:
        future = loop.run_in_executor(None,
            lambda: requests.get('https://jp1.chainweb.com/chainweb/0.0/mainnet01/cut/peer').json())
        resp = await future
        items = resp['items']
        items = [(host['address']['hostname'], host['address']['port']) for host in items]
        return items
    except KeyboardInterrupt as e:
        raise e
    except Exception as e:
        print(e)
        return []

async def get_peer_height(loop, host, port):
    height = None
    try:
        future = loop.run_in_executor(None,
            lambda: requests.get('https://%s:%s/chainweb/0.0/mainnet01/cut/' % (host, port),
                        verify=False, timeout=3))
        resp = await future
        height = resp.json()['height']
    except KeyboardInterrupt as e:
        raise e
    except:
        return None
    return height

async def get_peer_coordinator(loop, host, port):
    result = None
    try:
        future = loop.run_in_executor(None,
            lambda: requests.get(
                'https://%s:%s/chainweb/0.0/mainnet01/mining/work?chain=0' % (host, port),
                json={
                    'account': '1caecd6adeb2c2f8373e1d5d026d3a20c4b4448e95e8909d11772bdc72cd6d6f',
                    'public-keys': ["1caecd6adeb2c2f8373e1d5d026d3a20c4b4448e95e8909d11772bdc72cd6d6f"],
                    'predicate': "keys-all"
                },
                verify=False, timeout=3).text)
        resp = await future
        result = len(resp) != 0
    except KeyboardInterrupt as e:
        raise e
    except:
        return None
    return result

def generate_account_balance_request(account_name):
    with open('./tmp-request.yaml', 'wt+') as fout:
        fout.write("""
code: (coin.details "%s")
publicMeta:
  chainId: "-1"
  sender: "c18fa7134a3d5f7c0d2f965e7a3510d634d832e8c70293866bf983df443a9b57"
  gasLimit: 1000
  gasPrice: 0.01
  ttl: 100000
  creationTime: 0
networkId: "mainnet01"
keyPairs:
  - public: c18fa7134a3d5f7c0d2f965e7a3510d634d832e8c70293866bf983df443a9b57
    secret: a1b6db27d30d7a1c8505fdaf0efedd9c1facc135811bdaf3b544be9fd2a6e0b3
type: exec
        """ % (account_name))

    p = subprocess.run([PACT_TOOL, '-a', './tmp-request.yaml', '-l'], capture_output=True)

    req = json.loads(p.stdout)
    return req

async def get_balance(loop, account_name):
    req = generate_account_balance_request(account_name)

    chain_balances = []
    try:
        for chain in range(0, 10):
            future = loop.run_in_executor(None,
                lambda: requests.post(
                    url='https://jp1.chainweb.com/chainweb/0.0/mainnet01/chain/%s/pact/api/v1/local' % chain,
                    json=req))
            resp = await future
            js = resp.json()['result']
            if 'data' not in js:
                pass
            else:
                chain_balances.append((chain, js['data']['balance']))
            await asyncio.sleep(0.1)
    except:
        return None
    return chain_balances

BalanceTask = namedtuple('BalanceTask', 'channel author account')

# https://discordapp.com/api/oauth2/authorize?client_id=641529581618331650&scope=bot&permissions=19456

class KadenaChanClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())

    async def on_ready(self):
        print('Logged in as')
        print(client.user.name)
        print(client.user.id)
        self.working_like_hell = False
        self.work_queue = []
        self.peer_table = {}

    async def start_working(self):
        while len(self.work_queue) > 0:
            self.working_like_hell = True

            task = self.work_queue[0]

            if isinstance(task, BalanceTask):
                channel, author, account_name = task

                result = await get_balance(self.loop, account_name)
                print(author, account_name, result)

                if result is None:
                    msg = 'Sorry, {0.mention}, I am having some problems communicating with the network.'.format(author)
                    await channel.send(msg)
                elif result == []:
                    msg = 'Sorry, {0.mention}, are you sure that an account named {1} exists?'.format(author, account_name)
                    await channel.send(msg)
                else:
                    msg = 'Okay, {0.mention}, your total is ₭{1:,.1f}!'.format(author, sum(x for i, x in result))
                    await channel.send(msg)
                    msg = 'Your chain balances are: \n```haskell\n{0}\n```'.format(
                        '\n'.join("   Chain {0} - ₭{1:,.1f}".format(i, x) for i, x in result))
                    await channel.send(msg)
            else: assert False

            self.work_queue = self.work_queue[1:]
            self.working_like_hell = False


    async def on_message(self, message):
        # we do not want the bot to reply to itself
        if message.author == client.user:
            return

        if message.content.startswith('!balance'):
            arg_list = message.content.split(' ')
            if len(arg_list) <= 1:
                msg = '{0.author.mention}, love, I don\'t know your account name, tell me.'.format(message)
                await message.channel.send(msg)
            else:
                account_name = arg_list[1]
                account_name = ''.join(e for e in account_name if e.isalnum())

                if any(t.author.id == message.author.id for t in self.work_queue):
                    msg = '{0.author.mention}, shush, I am already working on something for you.'.format(message)
                    await message.channel.send(msg)
                else:
                    if len(self.work_queue) == 0 and not self.working_like_hell:
                        msg = 'Hey {0.author.mention}, let me fetch that for you real quick...'.format(message)
                        await message.channel.send(msg)
                    else:
                        msg = '{0.author.mention}, I will be with you in a minute, please wait!'.format(message)
                        await message.channel.send(msg)

                    task = BalanceTask(message.channel, message.author, account_name)
                    self.work_queue.append(task)
                    await self.start_working()

        if message.content.startswith('!heights'):
            ptable = sorted(self.peer_table.items(), key=lambda t: -t[1][0] if t[1][0] is not None else 0)
            ptable = ptable[:15]
            table = []
            for (host, port), (height, ip, coordinator) in ptable:

                if len(host) > 20:
                    host = host[:10] + ('... (%s)' % ip)

                table.append([str(x) for x in [
                    host + ':' + str(port),
                    height / 10 if height is not None else "????",
                    "True" if coordinator else "False"]])

            table = tabulate(table, headers=["Host:Port", "Height", "Mining Enabled"])

            await message.channel.send("Here we go (top 15 peers):\n```haskell\n" + table + "\n```\n")


    async def my_background_task(self):
        await self.wait_until_ready()

        while True:
            print("Getting peers.")
            try:
                all_peers = await get_peer_list(self.loop)

                for (host, port) in all_peers:
                    # print("Trying %s:%s" % (host, port))
                    if host[-1].isdigit():
                        ip = host
                    else:
                        ip = socket.gethostbyname(host)
                        # print("Ip = %s" % (ip))

                    # match = geolite2.lookup(ip)

                    # if match is not None:
                    #     country = match.country
                    # else:
                    #     country = "??"

                    # print("Country = %s" % country)

                    height = await get_peer_height(self.loop, host, port)
                    # print("Height = %s" % height)

                    coordinator = await get_peer_coordinator(self.loop, host, port)

                    self.peer_table[(host, port)] = (height, ip, coordinator)
            except Exception as e:
                print(e)
            # print("Bg task is sleeping.")
            await asyncio.sleep(30)

client = KadenaChanClient()
client.run(KADENA_CHAN_TOKEN)