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
wait_for = config["wait_for"]

SESSION_FOLDER = "sessions"
os.makedirs(SESSION_FOLDER, exist_ok=True)
app = Client("session", api_id=api_id, api_hash=api_hash, workdir=SESSION_FOLDER)
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
            logger.warning(f"FloodWait for {flood_wait.value}...")
            await asyncio.sleep(flood_wait.value)
            return sem_task(msg, chat_id)
          except SlowmodeWait as flood_wait:
            logger.warning(f"SlowmodeWait for {flood_wait.value}...")
            await asyncio.sleep(flood_wait.value)
            return sem_task(msg, chat_id)
          except Exception as e:
            tb_str = traceback.format_exc()
            logger.warning(f"failed to send message: {e} / {tb_str}")
            raise e

      tasks = []
      for chat_id in chat_ids:
        tasks.append(asyncio.create_task(sem_task(random.choice(messages), chat_id)))
        await asyncio.sleep(0.2)

      results = await asyncio.gather(*tasks, return_exceptions=True)
      suc = 0
      for res in results:
        if isinstance(res, BaseException):
          continue

        suc += 1
      logger.warning(f"sent to {suc}/{len(chat_ids)}, waiting {wait_for} secs...")
      await asyncio.sleep(wait_for)

if __name__ == "__main__":
  app.run(main())