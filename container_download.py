#!/usr/bin/env python3
import json
import os
import sys
import uuid
import time
from datetime import datetime, timedelta
import requests
from requests_xml import XMLSession
import asyncio
from aiohttp import ClientSession, TCPConnector

async def grabber(path, outputFile, ses):
    try:
        print('Grabbing: {} --> {}'.format(path, outputFile));
        async with ses.get(path) as r:
            print('have response')
            with open(outputFile, 'wb') as f:
                print('opened file -- {}'.format(outputFile))
                async for data, _ in r.content.iter_chunks():
                    if data: # filter out keep-alive new chunks
                        f.write(data)
    except Exception as e:
        print("here")
        print(e, file=sys.stderr)    

async def main():
    # get command line args
    account = sys.argv[1]
    container = sys.argv[2]
    sas = sys.argv[3]
    destDir = sys.argv[4]

    # list files in container
    try:
        session = XMLSession()
        requestUrl = 'https://{}.blob.core.windows.net/{}?restype=container&comp=list&{}'.format(account, container, sas)
        r = session.get(requestUrl)
        blobNames = [ e.text for e in r.xml.xpath('Blobs//Blob/Name') ]
    except Exception as e:
        print(e, file=sys.stderr)
    print('Blobs: {}'.format(blobNames))

    # download the blobs
    # https://myaccount.blob.core.windows.net/mycontainer/myblob
    try:
        tasks = []
        async with ClientSession(connector=TCPConnector(ssl=False)) as session:
            for blob in blobNames:
                filePath = os.path.join(destDir, blob)
                requestUrl = 'https://{}.blob.core.windows.net/{}/{}?{}'.format(account, container, blob, sas)
                task = asyncio.ensure_future(grabber(requestUrl, filePath, session))
                tasks.append(task)
            await asyncio.gather(*tasks)
    except Exception as e:
        print("oops")
        print(e, file=sys.stderr)
    except:
        print("Unexpected error:", sys.exc_info()[0])

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(main())
    loop.run_until_complete(future)