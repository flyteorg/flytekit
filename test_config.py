#!/usr/bin/env python

from flytekit.configuration import creds

secret = creds.CLIENT_CREDENTIALS_SECRET.get()
print("Secret is {}".format(secret))
