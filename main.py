import os
import json
import random
import asyncio
import traceback
from pyrogram import Client
from pyrogram.enums import ParseMode, ClientPlatform
from pyrogram.errors import FloodWait, SlowmodeWait
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
with open("config.json", "r", encoding="utf-8") as file:
  config = json.loads(file.read())

api_id = config["api_id"]
api_hash = config["api_hash"]
chat_ids = config["chat_ids"]
messages = config["messages"]
wait_for = config["wait_for"]
attachment_dir = config["attachment_dir"]

os.makedirs(attachment_dir, exist_ok=True)
SESSION_FOLDER = "sessions"
os.makedirs(SESSION_FOLDER, exist_ok=True)
app = Client("session", api_id=api_id, api_hash=api_hash, workdir=SESSION_FOLDER, device_model="autoadv", client_platform=ClientPlatform.ANDROID, app_version="Android 11.14.1")
async def main():
  async with app:
    for chat_id in chat_ids:
      await app.get_chat(int(chat_id))
      async for m in app.get_chat_history(int(chat_id), limit=1):
        pass
    
    while True:
      max_concurrency = 5
      semaphore = asyncio.Semaphore(max_concurrency)

      async def sem_task(msg, chat_id, attachment=None):
        async with semaphore:
          try:
            await app.send_photo(chat_id, photo=attachment, caption=msg, parse_mode=ParseMode.HTML)
          except FloodWait as flood_wait:
            logger.warning(f"FloodWait for {flood_wait.value}...")
            await asyncio.sleep(flood_wait.value)
            return sem_task(msg, chat_id, attachment=attachment)
          except SlowmodeWait as flood_wait:
            logger.warning(f"SlowmodeWait for {flood_wait.value}...")
            await asyncio.sleep(flood_wait.value)
            return sem_task(msg, chat_id, attachment=attachment)
          except Exception as e:
            tb_str = traceback.format_exc()
            logger.warning(f"failed to send message: {e} / {tb_str}")
            raise e

      tasks = []
      for chat_id in chat_ids:
        tasks.append(asyncio.create_task(sem_task(random.choice(messages), chat_id, attachment=random_file(attachment_dir))))
        await asyncio.sleep(0.2)

      results = await asyncio.gather(*tasks, return_exceptions=True)
      suc = 0
      for res in results:
        if isinstance(res, BaseException):
          continue

        suc += 1
      logger.warning(f"sent to {suc}/{len(chat_ids)}, waiting {wait_for} secs...")
      await asyncio.sleep(wait_for)

def list_image_files(directory, recursive=False, exts=None):
  if exts is None:
    exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp'}
  path = Path(directory)
  pattern = '**/*' if recursive else '*'
  return [
    p for p in path.rglob(pattern)
    if p.is_file() and p.suffix.lower() in exts
  ]

def random_file(directory: str) -> str:
  files = list_image_files(directory)
  random_file = random.choice(files)
  return str(random_file)

if __name__ == "__main__":
  app.run(main())