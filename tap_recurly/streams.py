
# 
# Module dependencies.
# 

import os
import json
import datetime
import pytz
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from dateutil.parser import parse
from tap_recurly.context import Context


logger = singer.get_logger()
KEY_PROPERTIES = ['id']


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def needs_parse_to_date(string):
    if isinstance(string, str):
        try: 
            parse(string)
            return True
        except ValueError:
            return False
    return False


class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = KEY_PROPERTIES
    session_bookmark = None


    def __init__(self, client=None):
        self.client = client


    def get_bookmark(self, state, name=None):
        name = self.name if not name else name
        return (singer.get_bookmark(state, name, self.replication_key)) or Context.config["start_date"]


    def update_bookmark(self, state, value, name=None):
        name = self.name if not name else name
        if self.is_bookmark_old(state, value, name):
            singer.write_bookmark(state, name, self.replication_key, value)


    def is_bookmark_old(self, state, value, name):
        current_bookmark = self.get_bookmark(state, name)
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(current_bookmark)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema


    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)


    def is_selected(self):
        return self.stream is not None


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)

        res = get_data(self.replication_key, bookmark)

        if self.replication_method == "INCREMENTAL":
            for item in res:
                self.update_bookmark(state, item[self.replication_key])
                yield (self.stream, item)

        elif self.replication_method == "FULL_TABLE":
            for item in res:
                yield (self.stream, item)

        else:
            raise Exception('Replication key not defined for {stream}'.format(self.name))
        

class Accounts(Stream):
    name = "accounts"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class BillingInfo(Stream):
    name = "billing_info"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "account_code" ]


class Adjustments(Stream):
    name = "adjustments"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class CouponRedemptions(Stream):
    name = "coupon_redemptions"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]
    parent_streams = [ "accounts", "subscriptions", "invoices" ]

    # It has it's own sync since it uses multiplpe parent streams.
    def sync(self, state):
        bookmark = {}
        for stream in self.parent_streams:
            name = "{stream}_{name}".format(stream=stream, name=self.name)
            get_data = getattr(self.client, name)
            res = get_data(self.replication_key, self.get_bookmark(state, name))
            for item in res:
                self.update_bookmark(state, item[self.replication_key], name)
                yield (self.stream, item)


class Coupons(Stream):
    name = "coupons"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class Invoices(Stream):
    name = "invoices"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class Plans(Stream):
    name = "plans"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class PlansAddOns(Stream):
    name = "plans_add_ons"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class Subscriptions(Stream):
    name = "subscriptions"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]


class Transactions(Stream):
    name = "transactions"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = [ "id" ]



STREAMS = {
    "accounts": Accounts,
    "billing_info": BillingInfo,
    "adjustments": Adjustments,
    "coupon_redemptions": CouponRedemptions,
    "coupons": Coupons,
    "invoices": Invoices,
    "plans": Plans,
    "plans_add_ons": PlansAddOns,
    "subscriptions": Subscriptions,
    "transactions": Transactions
}






