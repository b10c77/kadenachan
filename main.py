import discord
import asyncio
from geoip import geolite2
import socket
import requests
import subprocess
from subprocess import Popen, PIPE
import json
import random
import requests
import time
import logging
from tabulate import tabulate

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from collections import namedtuple


import sys
KADENA_CHAN_TOKEN = sys.argv[1]
PACT_TOOL = './pact'

logger = logging.getLogger('discord')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

# These nodes were THE ONLY properly working nodes on 10/08/2019 (properly
# configured with a certificate). Giving them some credit here.
BIGCHUNGA = [
    ('kadena1.block77.io', 443),
    ('kadena1.dzy.lv', 443),
    ('kadena1.banteg.xyz', 1337),
    ('kadena2.banteg.xyz', 1337),
    ('kadena420.taonacoin.com', 443),
    ('kadena69.taonacoin.com', 443),

    ('brudda.bigchungusmining.energy', 443),
    ('bigchungusmining.energy', 31337),
    ('bigchungusmining.energy', 31336),
    ('bigchungusmining.energy', 31335),
    ('bigchungusmining.energy', 31334),
    ('bigchungusmining.energy', 31333),
    ('bigchungusmining.energy', 31332),
    ('bigchungusmining.energy', 31331),
    ('bigchungusmining.energy', 31330),
                ]

BOOTSTRAPS = [
    ('jp1.chainweb.com', 443),
    ('jp2.chainweb.com', 443),
    ('jp3.chainweb.com', 443),
    ('fr1.chainweb.com', 443),
    ('fr2.chainweb.com', 443),
    ('fr3.chainweb.com', 443),
('us-e1.chainweb.com', 443),
    ('us-e2.chainweb.com', 443),
    ('us-e3.chainweb.com', 443),
    ('us-w1.chainweb.com', 443),
    ('us-w2.chainweb.com', 443),
]
async def get_peer_list(loop):
    for it in range(5):
        try:
            host, port = BOOTSTRAPS[random.randint(1, len(BOOTSTRAPS)) - 1]
            print('get_peer_list', host, port)
            future = loop.run_in_executor(None,
                lambda: requests.get('https://%s:%s/chainweb/0.0/mainnet01/cut/peer' % (host, port), verify=False).json())
            resp = await future
            items = resp['items']
            items = [(host['address']['hostname'], host['address']['port']) for host in items]
            return list(set(items + BOOTSTRAPS + BIGCHUNGA))
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            print(e)
    return []

async def get_peer_height(loop, host, port):
    height = None
    try:
        print('get_peer_height', host, port)
        future = loop.run_in_executor(None,
            lambda: requests.get('https://%s:%s/chainweb/0.0/mainnet01/cut/' % (host, port),
                        verify=False, timeout=15))
        resp = await future
        resp = resp.json()
        height = resp['height']
        weight = resp['weight']
        import base64
        weight = int.from_bytes(base64.urlsafe_b64decode(weight + '=='), byteorder='little')
    except KeyboardInterrupt as e:
        raise e
    except:
        return None
    return (height, weight)

async def get_peer_coordinator(loop, host, port):
    result = None
    try:
        print('get_peer_coordinator', host, port)
        future = loop.run_in_executor(None,
            lambda: requests.get(
                'https://%s:%s/chainweb/0.0/mainnet01/mining/work?chain=0' % (host, port),
                json={
                    'account': '1caecd6adeb2c2f8373e1d5d026d3a20c4b4448e95e8909d11772bdc72cd6d6f',
                    'public-keys': ["1caecd6adeb2c2f8373e1d5d026d3a20c4b4448e95e8909d11772bdc72cd6d6f"],
                    'predicate': "keys-all"
                },
                verify=False, timeout=15).text)
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

async def get_balance(loop, account_name, top_servers):
    req = generate_account_balance_request(account_name)

    top_servers = list(set(top_servers))

    chain_balances = {}

    for i, (host, port) in enumerate(top_servers):
        try:
            print('get_balance', host, port)
            for chain in range(0, 10):
                future = loop.run_in_executor(None,
                    lambda: requests.post(
                        url='https://%s:%s/chainweb/0.0/mainnet01/chain/%s/pact/api/v1/local' % (host, port, chain),
                        json=req, verify=False, timeout=20))
                resp = await future
                js = resp.json()['result']
                if 'data' not in js:
                    chain_balances.setdefault(chain, {})[i] = 0
                else:
                    chain_balances.setdefault(chain, {})[i] = (js['data']['balance'])

                if (host, port) in BOOTSTRAPS:
                    await asyncio.sleep(10)
                else:
                    await asyncio.sleep(0.1)
        except Exception as e:
            print(e)

    server_sum = {} # Per server
    chain_min = {} # Per chain
    chain_max = {} # Per chain

    for chain in range(0, 10):
        for server in range(len(top_servers)):
            val = chain_balances.get(chain, {}).get(server)
            if val is None:
                continue

            if server in server_sum:
                server_sum[server] = val + server_sum[server]
            else:
                server_sum[server] = val

            chain_min[chain] = min(chain_min.get(chain, val), val)
            chain_max[chain] = max(chain_max.get(chain, val), val)

    total_min = min(server_sum.values())
    total_max = max(server_sum.values())

    return chain_min, chain_max, total_min, total_max

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
        self.last_job = {}

    async def start_working(self):
        while len(self.work_queue) > 0:
            self.working_like_hell = True

            task = self.work_queue[0]

            if isinstance(task, BalanceTask):
                channel, author, account_name = task

                top_servers1 = sorted(self.peer_table.items(), key=lambda t: -t[1][-1] if t[1][-1] is not None else 0)
                top_servers1 = [x[0] for x in top_servers1 if x[0] in BIGCHUNGA]
                top_servers1 = top_servers1[:10]
                import random
                random.shuffle(top_servers1)
                top_servers1 = top_servers1[:5]

                top_servers2 = [BOOTSTRAPS[random.randint(1, len(BOOTSTRAPS)) - 1]]

                result = await get_balance(self.loop, account_name, top_servers1 + top_servers2)
                print(author, account_name, result)

                if result is None:
                    msg = 'Sorry, {0.mention}, I am having some problems communicating with the network.'.format(author)
                    await channel.send(msg)
                elif len(result[0]) == 0:
                    msg = 'Sorry, {0.mention}, are you sure that an account named {1} exists?'.format(author, account_name)
                    await channel.send(msg)
                else:
                    chain_min, chain_max, total_min, total_max = result
                    chain_keys = sorted(list(chain_min.keys() | chain_max.keys()))

                    msg = 'Okay, {0.mention}, the total for `{1}` is ₭{2:,.1f} ~ ₭{3:,.1f}!'.format(author, account_name, total_min, total_max)
                    await channel.send(msg)
                    msg = 'Individual chain balances are: \n```haskell\n{0}\n```'.format(
                        '\n'.join("   Chain {0} - ₭{1:,.1f} ~ ₭{2:,.1f}".format(i, chain_min[i], chain_max[i]) for i in chain_keys))
                    await channel.send(msg)
            else: assert False

            self.work_queue = self.work_queue[1:]
            self.working_like_hell = False


    async def on_message(self, message):
        # we do not want the bot to reply to itself
        if message.author == client.user:
            return

        if message.content.startswith('!balance'):
            author_id = str(message.author.mention)
            last_time = self.last_job.get(author_id)

            now_time = time.time()
            if last_time is not None and last_time >= now_time - 60 * 5:
                left_seconds = last_time - (now_time - 60 * 5)
                msg = 'Hey {0.author.mention}, please wait another {1} seconds'.format(message, left_seconds)
                await message.channel.send(msg)
                return

            self.last_job[author_id] = time.time()

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
            for (host, port), (height, ip, coordinator, country, weight) in ptable:

                if len(host) > 23:
                    host = host[:10] + ('... (%s)' % ip)

                table.append([str(x) for x in [
                    country,
                    host + ':' + str(port),
                    height / 10 if height is not None else "????",
                    weight if weight is not None else "????",
                    "True" if coordinator else "False"]])

            table = tabulate(table, headers=["Country", "Host:Port", "Height", "Weight", "MiningOn"])

            await message.channel.send("Here we go (top 15 peers):\n```haskell\n" + table + "\n```\n")

        if message.content.startswith('!weights'):
            ptable = sorted(self.peer_table.items(), key=lambda t: -t[1][-1] if t[1][-1] is not None else 0)
            table = []
            for (host, port), (height, ip, coordinator, country, weight) in ptable:
                if len(host) > 23:
                    host = host[:10] + ('... (%s)' % ip)

                table.append([str(x) for x in [
                    country,
                    host + ':' + str(port),
                    height / 10 if height is not None else "????",
                    weight if weight is not None else "????",
                    "True" if coordinator else "False"]])

            count = 20
            while True:
                table_str = tabulate(table[:count], headers=["Country", "Host:Port", "Height", "Weight", "MiningOn"])
                if len(table_str) > 1900:
                    count -= 1
                else:
                    break

            await message.channel.send(("Here we go (top %s peers):\n```haskell\n" % count) + table_str + "\n```\n")


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

                    match = geolite2.lookup(ip)

                    if match is not None:
                        country = match.country
                    else:
                        country = "??"

                    # print("Country = %s" % country)

                    hw = await get_peer_height(self.loop, host, port)
                    if hw is None:
                        height, weight = None, None
                    else:
                        height, weight = hw
                    # print("Height = %s" % height)

                    coordinator = await get_peer_coordinator(self.loop, host, port)

                    self.peer_table[(host, port)] = (height, ip, coordinator, country, weight)
            except Exception as e:
                print(e)
            # print("Bg task is sleeping.")
            await asyncio.sleep(30)

client = KadenaChanClient()
client.run(KADENA_CHAN_TOKEN)
