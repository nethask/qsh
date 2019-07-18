import io
import gzip
import struct
import datetime
import six
from collections import namedtuple
from dateutil import tz

def open(filename, mode="rb"):
    """Open QSH file in binary mode"""
    return QshFile(filename, mode)

def to_milliseconds(datetime_value):
    """Converts datetime value to milliseconds that have passed since 01.01.0001 00:00:00"""
    return (datetime_value - datetime.datetime(1, 1, 1)).total_seconds() * 1000

def to_datetime(milliseconds_value):
    """Converts milliseconds since 01.01.0001 00:00:00 to datetime value"""
    return datetime.datetime(1, 1, 1) + datetime.timedelta(milliseconds=milliseconds_value)

def is_available(list_mask, item_mask):
    """Checks if item are in list using mask"""
    return (list_mask & item_mask) != 0

class StreamType:
    QUOTES     = 16
    DEALS      = 32
    OWN_ORDERS = 48
    OWN_TRADES = 64
    MESSAGES   = 80
    AUX_INFO   = 96
    ORD_LOG    = 112

OrdLogNamedTuple = namedtuple(
    'OrdLogNamedTuple',
    'actions_mask,exchange_timestamp,exchange_order_id,order_price,amount,amount_rest,deal_id,deal_price,oi_after_deal',
)

class OrdLogEntry(OrdLogNamedTuple):
    class DataFlag:
        DATETIME          = 1
        ORDER_ID          = 2
        ORDER_PRICE       = 4
        AMOUNT            = 8
        ORDER_AMOUNT_REST = 16
        DEAL_ID           = 32
        DEAL_PRICE        = 64
        OI_AFTER_DEAL     = 128

    class ActionFlag:
        NON_ZERO_REPL_ACT  = 1
        FLOW_START         = 2
        ADD                = 4
        FILL               = 8
        BUY                = 16
        SELL               = 32
        SNAPSHOT           = 64
        QUOTE              = 128
        COUNTER            = 256
        NON_SYSTEM         = 512
        END_OF_TRANSACTION = 1024
        FILL_OR_KILL       = 2048
        MOVED              = 4096
        CANCELED           = 8192
        CANCELED_GROUP     = 16384
        CROSS_TRADE        = 32768

DealEntryNamedTuple = namedtuple(
    'DealEntryNamedTuple',
    'type,id,timestamp,price,volume,oi,order_id',
)

class DealEntry(DealEntryNamedTuple):
    class DataFlag:
        TYPE     = 3
        DATETIME = 4
        ID       = 8
        ORDER_ID = 16
        PRICE    = 32
        VOLUME   = 64
        OI       = 128

    class Type:
        UNKNOWN  = 0
        BUY      = 1
        SELL     = 2
        RESERVED = 3

AuxInfoNamedTuple = namedtuple(
    'AuxInfoNamedTuple',
    'timestamp,price,ask_total,bid_total,oi,hi_limit,low_limit,deposit,rate,message',
)

class AuxInfoEntry(AuxInfoNamedTuple):
    class DataFlag:
        DATETIME     = 1
        ASK_TOTAL    = 2
        BID_TOTAL    = 4
        OI           = 8
        PRICE        = 16
        SESSION_INFO = 32
        RATE         = 64
        MESSAGE      = 128

MessageNamedTuple = namedtuple(
    'MessageNamedTuple',
    'timestamp,type,text',
)

class Message(MessageNamedTuple):
    class Type:
        INFORMATION = 1
        WARNING     = 2
        ERROR       = 3

OwnTrade = namedtuple(
    'OwnTradeNamedTuple',
    'timestamp,trade_id,order_id,price,volume',
)

OwnOrderNamedTuple = namedtuple(
    'OwnOrderNamedTuple',
    'type,id,price,amount_rest',
)

class OwnOrder(OwnOrderNamedTuple):
    class DataFlag:
        DROP_ALL = 1
        ACTIVE   = 2
        EXTERNAL = 4
        STOP     = 8

    class Type:
        NONE    = 0
        REGULAR = 1
        STOP    = 2

class QshFileHeader:
    signature = "QScalp History Data"
    version = None
    application = None
    comment = None
    created_at = None
    streams_count = None

class QshFile:
    from_zone = tz.gettz("UTC")
    to_zone   = tz.gettz("Europe/Moscow")

    def __init__(self, filename=None, mode=None):
        self.fileobj = gzip.open(filename, mode)

        signature = self.read(len(QshFileHeader.signature.encode("utf8"))).decode('ascii')
        if QshFileHeader.signature != signature:
            self.fileobj = io.open(filename, mode)

            signature = self.read(len(QshFileHeader.signature.encode("utf8"))).decode('ascii')

            if QshFileHeader.signature != signature:
                raise TypeError("Unsupported file format")

        self.header = QshFileHeader()
        self.header.version = self.read_byte()
        self.header.application = self.read_string()
        self.header.comment = self.read_string()
        self.last_created_at_milliseconds = to_milliseconds(self.read_datetime())
        self.header.created_at = to_datetime(self.last_created_at_milliseconds).replace(tzinfo=self.from_zone).astimezone(self.to_zone)
        self.header.streams_count = self.read_byte()

        # Last values for read_ord_log_data()
        self.quotes = {}
        self.external_quotes = {}

        # Last values for read_quotes_data()
        self.quotes_last_price = 0
        self.quotes_dict = {}

        self.last_frame_milliseconds = self.last_created_at_milliseconds

    def __enter__(self):
        self.fileobj._checkClosed()
        return self

    def __exit__(self, *args):
        self.fileobj.close()

    def tell(self):
        """Return an int indicating the current stream position"""
        return self.fileobj.tell()

    def seek(self, position):
        """Change stream position"""
        self.fileobj.seek(position)

    def close(self):
        self.fileobj.close()

    def read(self, length):
        """Reads 'length' bytes from fileobj and raises EORError if end of the fileobj reached"""
        result = self.fileobj.read(length)

        if result == '': # OR b'' ?
            raise EOFError

        return result

    def read_uleb128(self):
        result = 0
        shift  = 0

        while True:
            byte = self.read_byte()

            result |= ((byte & 0x7F) << shift)

            if (byte & 0x80) == 0:
                break

            shift += 7

        return result

    def read_leb128(self):
        result = 0
        shift  = 0

        while True:
            byte = self.read_byte()

            result |= ((byte & 0x7F) << shift)

            shift += 7

            if (byte & 0x80) == 0:
                break

        if byte & 0x40:
            result |= -(1 << shift)

        return result

    def read_string(self):
        length = self.read_uleb128()
        return self.read(length).decode('ascii')

    def read_int64(self):
        return struct.unpack("q", self.read(8))[0]

    def read_datetime(self):
        nanoseconds = self.read_int64()
        return datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=nanoseconds//10)

    def read_byte(self):
        return struct.unpack("B", self.read(1))[0]

    def read_uint16(self):
        return struct.unpack("H", self.read(2))[0]

    # Test it!
    def read_uint32(self):
        return struct.unpack("I", self.read(4))[0]

    # Test it!
    def read_double(self):
        return struct.unpack("d", self.read(8))[0]

    def read_relative(self, last_value):
        offset = self.read_leb128()

        return last_value + offset

    def read_growing(self, last_value):
        offset = self.read_uleb128()

        if offset == 268435455:
            offset = self.read_leb128()

        return last_value + offset

    def read_growing_datetime(self, last_value):
        milliseconds = self.read_growing(last_value)

        return datetime.datetime(1, 1, 1) + datetime.timedelta(milliseconds=milliseconds)

    def read_stream_header(self):
        stream_type = self.read_byte()

        if stream_type == StreamType.MESSAGES:
            return stream_type, None

        instrument_code = self.read_string()

        return stream_type, instrument_code

    def read_frame_header(self):
        timestamp = self.read_growing_datetime(self.last_frame_milliseconds)
        self.last_frame_milliseconds = to_milliseconds(timestamp)
        timestamp = timestamp.replace(tzinfo=self.from_zone).astimezone(self.to_zone)

        if self.header.streams_count > 1:
            stream_index = self.read_byte()
            return timestamp, stream_index

        return timestamp, 0

    # Last values for read_ord_log_data()
    last_exchange_milliseconds = 0
    last_order_id              = 0
    last_order_price           = 0
    last_amount                = 0
    last_order_amount_rest     = 0
    last_deal_id               = 0
    last_deal_price            = 0
    last_oi_after_deal         = 0

    last_pushed_deal_id        = 0

    def read_ord_log_data(self):
        """Reads order log data from file"""
        availability_mask = self.read_byte()
        actions_mask      = self.read_uint16()

        is_add  = is_available(actions_mask, OrdLogEntry.ActionFlag.ADD)
        is_buy  = is_available(actions_mask, OrdLogEntry.ActionFlag.BUY)
        is_sell = is_available(actions_mask, OrdLogEntry.ActionFlag.SELL)

        if is_available(availability_mask, OrdLogEntry.DataFlag.DATETIME):
            self.last_exchange_milliseconds = to_milliseconds(self.read_growing_datetime(self.last_exchange_milliseconds))

        exchange_timestamp = to_datetime(self.last_exchange_milliseconds).replace(tzinfo=self.to_zone)

        if not is_available(availability_mask, OrdLogEntry.DataFlag.ORDER_ID):
            exchange_order_id = self.last_order_id
        elif is_add:
            self.last_order_id = self.read_growing(self.last_order_id)
            exchange_order_id = self.last_order_id
        else:
            exchange_order_id = self.read_relative(self.last_order_id)

        if is_available(availability_mask, OrdLogEntry.DataFlag.ORDER_PRICE):
            self.last_order_price = self.read_relative(self.last_order_price)

        if is_available(availability_mask, OrdLogEntry.DataFlag.AMOUNT):
            self.last_amount = self.read_leb128()

        if is_available(actions_mask, OrdLogEntry.ActionFlag.FILL):
            if is_available(availability_mask, OrdLogEntry.DataFlag.ORDER_AMOUNT_REST):
                self.last_order_amount_rest = self.read_leb128()

            amount_rest = self.last_order_amount_rest

            if is_available(availability_mask, OrdLogEntry.DataFlag.DEAL_ID):
                self.last_deal_id = self.read_growing(self.last_deal_id)

            deal_id = self.last_deal_id

            if is_available(availability_mask, OrdLogEntry.DataFlag.DEAL_PRICE):
                self.last_deal_price = self.read_relative(self.last_deal_price)

            deal_price = self.last_deal_price

            if is_available(availability_mask, OrdLogEntry.DataFlag.OI_AFTER_DEAL):
                self.last_oi_after_deal = self.read_relative(self.last_oi_after_deal)

            oi_after_deal = self.last_oi_after_deal
        else:
            amount_rest = self.last_amount if is_add else 0
            deal_id = 0
            deal_price = 0
            oi_after_deal = 0

        ord_log_entry  = OrdLogEntry(actions_mask, exchange_timestamp, exchange_order_id, self.last_order_price, self.last_amount, amount_rest, deal_id, deal_price, oi_after_deal)
        deal_entry     = None
        aux_info_entry = None

        if is_available(actions_mask, OrdLogEntry.ActionFlag.FLOW_START):
            self.quotes = {}

        if (is_buy ^ is_sell) and (not is_available(actions_mask, OrdLogEntry.ActionFlag.NON_SYSTEM)) and (not is_available(actions_mask, OrdLogEntry.ActionFlag.NON_ZERO_REPL_ACT)):
            # Updating order book
            quantity = self.quotes.get(self.last_order_price, 0)

            if (is_sell if is_add else is_buy):
                quantity += self.last_amount
            else:
                quantity -= self.last_amount

            if quantity == 0:
                self.quotes.pop(self.last_order_price, None)
            else:
                self.quotes[self.last_order_price] = quantity

            if is_available(actions_mask, OrdLogEntry.ActionFlag.END_OF_TRANSACTION):
                self.external_quotes = dict(self.quotes)

                ask_total = 0
                bid_total = 0

                for key, value in six.iteritems(self.quotes):
                    if value > 0:
                        ask_total += value
                    else:
                        bid_total -= value

                aux_info_entry = AuxInfoEntry(exchange_timestamp, self.last_deal_price, ask_total, bid_total, self.last_oi_after_deal, 0, 0, 0, 0, "")

            if self.last_pushed_deal_id < deal_id:
                self.last_pushed_deal_id = deal_id
                deal_type = DealEntry.Type.SELL if is_sell else DealEntry.Type.BUY
                deal_entry = DealEntry(deal_type, deal_id, exchange_timestamp, deal_price, self.last_amount, oi_after_deal, 0)

        return ord_log_entry, aux_info_entry, self.external_quotes, deal_entry

    def read_message_data(self):
        """Reads message data from file"""
        message_timestamp = self.read_datetime().replace(tzinfo=self.to_zone)
        message_type      = self.read_byte()
        message_text      = self.read_string()

        return Message(message_timestamp, message_type, message_text)

    def read_quotes_data(self):
        """Reads quotes data from file"""
        count = self.read_leb128()

        for i in range(count):
            self.quotes_last_price = self.read_relative(self.quotes_last_price)
            volume = self.read_leb128()

            if volume == 0:
                del self.quotes_dict[self.quotes_last_price]
            else:
                self.quotes_dict[self.quotes_last_price] = volume

        return dict(self.quotes_dict)

    # Last values for read_deals_data()
    deals_last_milliseconds = 0
    deals_last_id           = 0
    deals_last_order_id     = 0
    deals_last_price        = 0
    deals_last_oi           = 0

    def read_deals_data(self):
        """Reads deals data from file"""
        availability_mask = self.read_byte()

        deal_type = availability_mask & DealEntry.DataFlag.TYPE

        if is_available(availability_mask, DealEntry.DataFlag.DATETIME):
            self.deals_last_milliseconds = to_milliseconds(self.read_growing_datetime(self.deals_last_milliseconds))

        if is_available(availability_mask, DealEntry.DataFlag.ID):
            self.deals_last_id = self.read_growing(self.deals_last_id)

        if is_available(availability_mask, DealEntry.DataFlag.ORDER_ID):
            self.deals_last_order_id = self.read_relative(self.deals_last_order_id)

        if is_available(availability_mask, DealEntry.DataFlag.PRICE):
            self.deals_last_price = self.read_relative(self.deals_last_price)

        if is_available(availability_mask, DealEntry.DataFlag.VOLUME):
            self.deals_last_volume = self.read_leb128()

        if is_available(availability_mask, DealEntry.DataFlag.OI):
            self.deals_last_oi = self.read_relative(self.deals_last_oi)

        deal_timestamp = to_datetime(self.deals_last_milliseconds).replace(tzinfo=self.to_zone)

        return DealEntry(deal_type, self.deals_last_id, deal_timestamp, self.deals_last_price, self.deals_last_volume, self.deals_last_oi, self.deals_last_order_id)

    # Last values for read_auxinfo_data()
    auxinfo_last_milliseconds = 0
    auxinfo_last_ask_total    = 0
    auxinfo_last_bid_total    = 0
    auxinfo_last_oi           = 0
    auxinfo_last_price        = 0
    auxinfo_last_hi_limit     = 0
    auxinfo_last_low_limit    = 0
    auxinfo_last_deposit      = 0
    auxinfo_last_rate         = 0

    def read_auxinfo_data(self):
        """Reads auxinfo data from file"""
        availability_mask = self.read_byte()

        if is_available(availability_mask, AuxInfoEntry.DataFlag.DATETIME):
            self.auxinfo_last_milliseconds = to_milliseconds(self.read_growing_datetime(self.auxinfo_last_milliseconds))

        if is_available(availability_mask, AuxInfoEntry.DataFlag.ASK_TOTAL):
            self.auxinfo_last_ask_total = self.read_relative(self.auxinfo_last_ask_total)

        if is_available(availability_mask, AuxInfoEntry.DataFlag.BID_TOTAL):
            self.auxinfo_last_bid_total = self.read_relative(self.auxinfo_last_bid_total)

        if is_available(availability_mask, AuxInfoEntry.DataFlag.OI):
            self.auxinfo_last_oi = self.read_relative(self.auxinfo_last_oi)

        if is_available(availability_mask, AuxInfoEntry.DataFlag.PRICE):
            self.auxinfo_last_price = self.read_relative(self.auxinfo_last_price)

        if is_available(availability_mask, AuxInfoEntry.DataFlag.SESSION_INFO):
            self.auxinfo_last_hi_limit  = self.read_leb128()
            self.auxinfo_last_low_limit = self.read_leb128()
            self.auxinfo_last_deposit   = self.read_double()

        if is_available(availability_mask, AuxInfoEntry.DataFlag.RATE):
            self.auxinfo_last_rate = self.read_double()

        if is_available(availability_mask, AuxInfoEntry.DataFlag.MESSAGE):
            message = self.read_string()
        else:
            message = ""

        timestamp = to_datetime(self.auxinfo_last_milliseconds).replace(tzinfo=self.to_zone)

        return AuxInfoEntry(timestamp, self.auxinfo_last_price, self.auxinfo_last_ask_total, self.auxinfo_last_bid_total, self.auxinfo_last_oi, self.auxinfo_last_hi_limit, self.auxinfo_last_low_limit, self.auxinfo_last_deposit, self.auxinfo_last_rate, message)

    def read_own_orders_data(self):
        """Reads own orders data from file"""
        availability_mask = self.read_byte()

        if is_available(availability_mask, OwnOrder.DataFlag.DROP_ALL):
            return None

        order_type = OwnOrder.Type.NONE

        if is_available(availability_mask, OwnOrder.DataFlag.ACTIVE):
            if is_available(availability_mask, OwnOrder.DataFlag.STOP):
                order_type = OwnOrder.Type.STOP
            else:
                order_type = OwnOrder.Type.REGULAR

        order_id    = self.read_leb128()
        order_price = self.read_leb128()
        amount_rest = self.read_leb128()

        return OwnOrder(order_type, order_id, order_price, amount_rest)

    # Last values for read_own_trades_data()
    own_trades_last_milliseconds = 0
    own_trades_last_trade_id     = 0
    own_trades_last_order_id     = 0
    own_trades_last_price        = 0

    def read_own_trades_data(self):
        """Reads own trades data from file"""
        self.own_trades_last_milliseconds = to_milliseconds(self.read_growing_datetime(self.own_trades_last_milliseconds))

        self.own_trades_last_trade_id = self.read_relative(self.own_trades_last_trade_id)
        self.own_trades_last_order_id = self.read_relative(self.own_trades_last_order_id)
        self.own_trades_last_price    = self.read_relative(self.own_trades_last_price)

        volume = self.read_leb128()
        timestamp = to_datetime(self.own_trades_last_milliseconds).replace(tzinfo=self.to_zone)

        return OwnTrade(timestamp, self.own_trades_last_trade_id, self.own_trades_last_order_id, self.own_trades_last_price, volume)
