#!/usr/bin/env python3
import json
import os
import sys
import uuid
import time
from datetime import datetime, timedelta
from azure.storage.blob import BlockBlobService, ContainerPermissions

def main():
    # get command line args
    account = sys.argv[1]
    secret = sys.argv[2]
    srcContainer = sys.argv[3]
    files = sys.argv[4:]

    # generate container name
    destContainer = str(uuid.uuid4()).replace('-', '')

    try:
        # connect to blob store
        bs = BlockBlobService(account_name=account, account_key=secret)

        # create and setup container, by default a container is private
        bs.create_container(destContainer)
        bs.set_container_acl(destContainer)

        # perform blob copy
        copyStartTime = int(round(time.time() * 1000))
        copyProps = {}
        for f in files:
            srcUrl = 'https://{}.blob.core.windows.net/{}/{}'.format(account, srcContainer, f)
            cp = bs.copy_blob(destContainer, f, srcUrl)
            copyProps[f] = cp

        # wait for copy to finish
        while len(copyProps.keys()) > 0:
            for f, prop in copyProps.items():
                bp = bs.get_blob_properties(destContainer, f)
                copyProps[f] = None if bp.properties.copy.status is not 'pending' else bp
            copyProps = { k:v for k, v in copyProps.items() if v }
        
        # copy completed
        copyEndTime = int(round(time.time() * 1000))
        print('Blob copy completed in {}ms'.format(copyEndTime - copyStartTime), file=sys.stderr)

        # generate SAS token, read only, valid for an hour
        token = bs.generate_container_shared_access_signature(destContainer, ContainerPermissions.READ, datetime.utcnow() + timedelta(hours=1))

        # return information
        result = {
            'storage_account': account,
            'container': destContainer,
            'sas_token': token
        }
        print(json.dumps(result, indent=4, sort_keys=True))

    except Exception as e:
        print(e, file=sys.stderr)

if __name__ == "__main__":
    main()