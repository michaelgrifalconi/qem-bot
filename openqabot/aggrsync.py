# Copyright SUSE LLC
# SPDX-License-Identifier: MIT
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger

from .errors import EmptySettings
from .loader.config import read_products
from .loader.qem import get_aggregate_settings_data
from .syncres import SyncRes

log = getLogger("bot.aggrsync")


class AggregateResultsSync(SyncRes):
    operation = "aggregate"

    def __init__(self, args: Namespace) -> None:
        super().__init__(args)
        self.product = read_products(args.configs)

    def _get_results_from_openqa(self):
        update_setting = []
        log.info("PRODUCT-")
        log.info(self.product) #TODO: remove
        for product in self.product:
            try:
                update_setting += get_aggregate_settings_data(self.token, product)
            except EmptySettings as e:
                log.info(e)
                continue
        log.info("UPDATE_SETTING-")
        log.info(update_setting) #TODO: remove
        job_results = {}
        with ThreadPoolExecutor() as executor:
            future_j = {
                executor.submit(self.client.get_jobs, f): f for f in update_setting
            }
            for future in as_completed(future_j):
                job_results[future_j[future]] = future.result()

        results = []
        for key, values in job_results.items():
            for v in values:
                if not self.filter_jobs(v):
                    continue

                try:
                    r = self.normalize_data(key, v)
                except KeyError:
                    continue

                results.append(r)
        return results
        
    def __call__(self) -> int:


        results = self.get_results_from_openqa(self)
        for r in results:
            self.post_result(r)

        log.info("End of bot run")

        return 0
