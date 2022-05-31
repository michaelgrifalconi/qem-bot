# Copyright SUSE LLC
# SPDX-License-Identifier: MIT
from argparse import Namespace
from logging import getLogger
from typing import Sequence

from .loader.qem import get_active_incidents, get_incident_settings_data
from .syncres import SyncRes
from .types import Data

logger = getLogger("bot.incsyncres")


class IncResultsSync(SyncRes):
    operation = "incident"

    def __init__(self, args: Namespace) -> None:
        super().__init__(args)
        self.active = get_active_incidents(self.token)

    def __call__(self) -> int:

        incidents: Sequence[Data] = []

        for inc in self.active:
            try:
                incidents += get_incident_settings_data(self.token, inc)
            except ValueError:
                continue

        full = {}

        for d in incidents:
            full[d] = self.client.get_jobs(d)

        results = []
        for key, value in full.items():
            for v in value:
                if not self.filter_jobs(v):
                    continue

                try:
                    r = self.normalize_data(key, v)
                except KeyError:
                    continue

                results.append(r)

        for r in results:
            self.post_result(r)

        logger.info("End of bot run")

        return 0
