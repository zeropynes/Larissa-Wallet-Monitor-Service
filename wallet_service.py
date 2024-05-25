import asyncio
import aiohttp
import os
import json
import sqlite3
from datetime import datetime
import time

class WalletManager:
    def __init__(self, my_config_file: str):
        self.token = ''
        self.wallets = {}
        if os.path.exists("config.private.json"):
            config_file = "config.private.json"
        else:
            config_file = my_config_file
        self.load_config(config_file)
        self.initialize_db()

    def load_config(self, config_file: str) -> None:
        with open(config_file, 'r') as file:
            config = json.load(file)
            self.token = config["token"]

    def initialize_db(self):
        conn = sqlite3.connect('wallet_monitor.db')
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS wallet_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL,
            unclaimed_earnings REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS wallet_name (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_id TEXT NOT NULL UNIQUE,
            wallet_name TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        conn.commit()
        conn.close()

    async def fetch_wallet_data(self):
        url = "https://api.larissa.network/api/v1/wallet/getWallets"
        headers = {"Authorization": f"Bearer {self.token}"}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['status']:
                        for wallet in data['data']:
                            wallet_id = wallet["walletID"]
                            wallet_name = wallet["walletNodeName"]
                            self.wallets[wallet_id] = wallet_name
                    else:
                        print(f"Failed to fetch wallet data: {data['message']}")
                else:
                    print(f"Failed to fetch wallet data with status code:", response.status)

    async def get_wallet_earnings(self, session: aiohttp.ClientSession, wallet_id: str) -> float:
        url = "https://api.larissa.network/api/v1/key/keyUnclaimedEarning"
        headers = {"Authorization": f"Bearer {self.token}"}
        body = {"walletID": wallet_id}

        async with session.post(url, headers=headers, json=body) as response:
            if response.status == 200:
                data = await response.json()
                if data['status']:
                    return float(data['data'])
                else:
                    print(f"Failed for node {self.wallets[wallet_id]}: {data['message']}")
            else:
                print(f"Failed for node {self.wallets[wallet_id]} with status code:", response.status)
            return 0.0

    def append_wallet_history(self, wallet_id, unclaimed_earnings):
        conn = sqlite3.connect('wallet_monitor.db')
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO wallet_updates (wallet_id, unclaimed_earnings, updated_at)
        VALUES (?, ?, ?)
        ''', (wallet_id, unclaimed_earnings, datetime.now()))

        conn.commit()
        conn.close()

    def append_wallet_name(self, wallet_id, wallet_name):
        conn = sqlite3.connect('wallet_monitor.db')
        cursor = conn.cursor()

        cursor.execute('''
        INSERT OR REPLACE INTO 
            wallet_name (wallet_id, wallet_name, updated_at)
        VALUES (?, ?, ?)
        ''', (wallet_id, wallet_name, datetime.now()))

        conn.commit()
        conn.close()

    async def update_wallet_info(self):
        async with aiohttp.ClientSession() as session:
            await self.fetch_wallet_data()
            for wallet_id, wallet_name in self.wallets.items():
                earnings = await self.get_wallet_earnings(session, wallet_id)
                self.append_wallet_history(wallet_id, earnings)
                self.append_wallet_name(wallet_id, wallet_name)

    async def run(self):
        while True:
            await self.update_wallet_info()
            # Repeat every 5 minutes
            await asyncio.sleep(300)

if __name__ == "__main__":
    manager = WalletManager("config.json")
    asyncio.run(manager.run())
