import os
import json
import dotenv
import random
import asyncio
import traceback
from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, SlowmodeWait
import logging

logger = logging.getLogger(__name__)
with open("config.json") as file:
  config = json.loads(file)

api_id = config["api_id"]
api_hash = config["api_hash"]
chat_ids = config["chat_ids"]
messages = config["messages"]

app = Client("test", api_id=api_id, api_hash=api_hash, workdir="sessions")
async def main():
  async with app:
    for chat_id in chat_ids:
      await app.get_chat(int(chat_id))
      async for m in app.get_chat_history(int(chat_id), limit=1):
        pass
    
    while True:
      max_concurrency = 5
      semaphore = asyncio.Semaphore(max_concurrency)

      async def sem_task(msg, chat_id):
        async with semaphore:
          try:
            await app.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
          except FloodWait as flood_wait:
            print(f"FloodWait for {flood_wait.value}...")
            await asyncio.sleep(flood_wait.value)
            return sem_task(msg, chat_id)
          except SlowmodeWait as flood_wait:
            print(f"SlowmodeWait for {flood_wait.value}...")
            await asyncio.sleep(flood_wait.value)
            return sem_task(msg, chat_id)
          except Exception as e:
            tb_str = traceback.format_exc()
            print(f"failed to send message: {e} / {tb_str}")

      tasks = []
      for chat_id in chat_ids:
        tasks.append(asyncio.create_task(sem_task(random.choice(messages), chat_id)))
        await asyncio.sleep(0.2)

      results = await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
  app.run(main())