import time
import asyncio
async def a():
    await time.time()
    await time.sleep(2)
    await time.time()
async def b():

    await time.time()
    await time.sleep(2)
    await time.time()

def main():
    asyncio.Task(a())
    asyncio.Task(b())
    asyncio.get_event_loop().run_forever()


main()