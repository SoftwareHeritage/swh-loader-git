#!/usr/bin/env python3

import random
from retrying import retry


@retry
def pick_one():
    r = random.randint(0, 10)
    print(r)
    if r != 1:
        raise Exception("1 was not picked")


if __name__ == '__main__':
    pick_one()
