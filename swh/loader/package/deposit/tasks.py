# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from swh.loader.package.deposit.loader import DepositLoader


@shared_task(name=__name__ + ".LoadDeposit")
def load_deposit(*, url, deposit_id):
    """Load Deposit artifacts"""
    return DepositLoader.from_configfile(url=url, deposit_id=deposit_id).load()
