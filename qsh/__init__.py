import gzip
import struct
import datetime

def open(filename, mode="rb"):
    """Open QSH file in binary mode"""
    return QshFile(filename, mode)

def to_milliseconds(datetime_value):
    """Converts datetime value to milliseconds that have passed since 01.01.0001 00:00:00"""
    return (datetime_value - datetime.datetime(1, 1, 1)).total_seconds() * 1000

def to_datetime(milliseconds_value):
    """Converts milliseconds since 01.01.0001 00:00:00 to datetime value"""
    return datetime.datetime(1, 1, 1) + datetime.timedelta(milliseconds=milliseconds_value)

def available(list_mask, item_mask):
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

class OrdLogDataMask:
    DATETIME          = 1
    ORDER_ID          = 2
    ORDER_PRICE       = 4
    AMOUNT            = 8
    ORDER_AMOUNT_REST = 16
    DEAL_ID           = 32
    DEAL_PRICE        = 64
    OI_AFTER_DEAL     = 128

class OrdLogActionMask:
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

class DealDataMask:
    TYPE     = 3
    DATETIME = 4
    ID       = 8
    ORDER_ID = 16
    PRICE    = 32
    VOLUME   = 64
    OI       = 128

class AuxInfoDataMask:
    DATETIME     = 1
    ASK_TOTAL    = 2
    BID_TOTAL    = 4
    OI           = 8
    PRICE        = 16
    SESSION_INFO = 32
    RATE         = 64
    MESSAGE      = 128

class OwnOrderDataMask:
    DROP_ALL = 1
    ACTIVE   = 2
    EXTERNAL = 4
    STOP     = 8

class OrdLogEntry:
    def __init__(self, actions_mask, exchange_timestamp, exchange_order_id, order_price, amount, amount_rest, deal_id, deal_price, oi_after_deal):
        self.actions_mask       = actions_mask
        self.exchange_timestamp = exchange_timestamp
        self.exchange_order_id  = exchange_order_id
        self.order_price        = order_price
        self.amount             = amount
        self.amount_rest        = amount_rest
        self.deal_id            = deal_id
        self.deal_price         = deal_price
        self.oi_after_deal      = oi_after_deal

class DealEntry:
    class Type:
        UNKNOWN  = 0
        BUY      = 1
        SELL     = 2
        RESERVED = 3

    def __init__(self, deal_type, deal_id, timestamp, price, volume, oi, order_id):
        self.type      = deal_type
        self.id        = deal_id
        self.timestamp = timestamp
        self.price     = price
        self.volume    = volume
        self.oi        = oi
        self.order_id  = order_id

class AuxInfoEntry:
    def __init__(self, timestamp, price, ask_total, bid_total, oi, hi_limit, low_limit, deposit, rate, message):
        self.timestamp = timestamp
        self.price     = price
        self.ask_total = ask_total
        self.bid_total = bid_total
        self.oi        = oi
        self.hi_limit  = hi_limit
        self.low_limit = low_limit
        self.deposit   = deposit
        self.rate      = rate
        self.message   = message

class Message:
    class Type:
        INFORMATION = 1
        WARNING     = 2
        ERROR       = 3

    def __init__(self, timestamp, message_type, text):
        self.timestamp = timestamp
        self.type      = message_type
        self.text      = text

class OwnTrade:
    def __init__(self, timestamp, trade_id, order_id, price, volume):
        self.timestamp = timestamp
        self.trade_id  = trade_id
        self.order_id  = order_id
        self.price     = price
        self.volume    = volume

class OwnOrder:
    class Type:
        NONE    = 0
        REGULAR = 1
        STOP    = 2

    def __init__(self, order_type, order_id, price, amount_rest):
        self.type        = order_type
        self.id          = order_id
        self.price       = price
        self.amount_rest = amount_rest

class QshFile:

    class header:
        signature     = "QScalp History Data"
        version       = None
        application   = None
        comment       = None
        created_at    = None
        streams_count = None

    fileobj = None

    def __init__(self, filename=None, mode=None):
        self.fileobj = gzip.open(filename)

        signature = self.read(len(self.header.signature.encode("utf8")))
    
        if self.header.signature != signature:
            raise TypeError("Unsupported file format")

        self.header.version       = self.read_byte()
        self.header.application   = self.read_string()
        self.header.comment       = self.read_string()
        self.header.created_at    = self.read_datetime()
        self.header.streams_count = self.read_byte()
            
    def __enter__(self):
        self.fileobj._checkClosed()
        return self

    def __exit__(self, *args):
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
        return self.read(length)

    def read_int64(self):
        return struct.unpack("q", self.read(8))[0]

    def read_datetime(self):
        nanoseconds = self.read_int64()
        return datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=nanoseconds/10)

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

    # Last values for read_frame_header()
    last_frame_milliseconds = None

    def read_frame_header(self):
        if self.last_frame_milliseconds is None:
            self.last_frame_milliseconds = to_milliseconds(self.header.created_at)

        timestamp = self.read_growing_datetime(self.last_frame_milliseconds)
        self.last_frame_milliseconds = to_milliseconds(timestamp)

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

    quotes = {}
    external_quotes = {}

    def read_ord_log_data(self):
        """Reads order log data from file"""
        availability_mask = self.read_byte()
        actions_mask      = self.read_uint16()

        is_add  = True if available(actions_mask, OrdLogActionMask.ADD) else False
        is_fill = True if available(actions_mask, OrdLogActionMask.FILL) else False
        is_buy  = True if available(actions_mask, OrdLogActionMask.BUY) else False
        is_sell = True if available(actions_mask, OrdLogActionMask.SELL) else False

        if available(availability_mask, OrdLogDataMask.DATETIME):
            self.last_exchange_milliseconds = to_milliseconds(self.read_growing_datetime(self.last_exchange_milliseconds))

        exchange_timestamp = to_datetime(self.last_exchange_milliseconds)

        if not available(availability_mask, OrdLogDataMask.ORDER_ID):
            exchange_order_id = self.last_order_id
        elif is_add:
            self.last_order_id = self.read_growing(self.last_order_id)
            exchange_order_id = self.last_order_id
        else:
            exchange_order_id = self.read_relative(self.last_order_id)

        if available(availability_mask, OrdLogDataMask.ORDER_PRICE):
            self.last_order_price = self.read_relative(self.last_order_price)

        if available(availability_mask, OrdLogDataMask.AMOUNT):
            self.last_amount = self.read_leb128()

        if is_fill:
            if available(availability_mask, OrdLogDataMask.ORDER_AMOUNT_REST):
                self.last_order_amount_rest = self.read_leb128()
            
            amount_rest = self.last_order_amount_rest

            if available(availability_mask, OrdLogDataMask.DEAL_ID):
                self.last_deal_id = self.read_growing(self.last_deal_id)
                
            deal_id = self.last_deal_id
            
            if available(availability_mask, OrdLogDataMask.DEAL_PRICE):
                self.last_deal_price = self.read_relative(self.last_deal_price)

            deal_price = self.last_deal_price
            
            if available(availability_mask, OrdLogDataMask.OI_AFTER_DEAL):
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

        if available(actions_mask, OrdLogActionMask.FLOW_START):
            self.quotes = {}

        if (is_buy ^ is_sell) and (not available(actions_mask, OrdLogActionMask.NON_SYSTEM)) and (not available(actions_mask, OrdLogActionMask.NON_ZERO_REPL_ACT)):
            # Updating order book
            quantity = 0
            try:
                quantity = self.quotes[self.last_order_price]
            except KeyError:
                pass

            if (is_sell if is_add else is_buy):
                quantity += self.last_amount
            else:
                quantity -= self.last_amount

            if quantity == 0:
                del self.quotes[self.last_order_price]
            else:
                self.quotes[self.last_order_price] = quantity

            if available(actions_mask, OrdLogActionMask.END_OF_TRANSACTION):
                self.external_quotes = dict(self.quotes)

                ask_total = 0
                bid_total = 0

                for key, value in self.quotes.iteritems():
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
        message_timestamp = self.read_datetime()
        message_type      = self.read_byte()
        message_text      = self.read_string()

        return Message(message_timestamp, message_type, message_text)

    # Last values for read_quotes_data()
    quotes_last_price = 0
    quotes_dict       = {}

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

        deal_type = availability_mask & DealDataMask.TYPE

        if available(availability_mask, DealDataMask.DATETIME):
            self.deals_last_milliseconds = to_milliseconds(self.read_growing_datetime(self.deals_last_milliseconds))

        if available(availability_mask, DealDataMask.ID):
            self.deals_last_id = self.read_growing(self.deals_last_id)

        if available(availability_mask, DealDataMask.ORDER_ID):
            self.deals_last_order_id = self.read_relative(self.deals_last_order_id)

        if available(availability_mask, DealDataMask.PRICE):
            self.deals_last_price = self.read_relative(self.deals_last_price)

        if available(availability_mask, DealDataMask.VOLUME):
            self.deals_last_volume = self.read_leb128()

        if available(availability_mask, DealDataMask.OI):
            self.deals_last_oi = self.read_relative(self.deals_last_oi)

        return DealEntry(deal_type, self.deals_last_id, to_datetime(self.deals_last_milliseconds), self.deals_last_price, self.deals_last_volume, self.deals_last_oi, self.deals_last_order_id)

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

        if available(availability_mask, AuxInfoDataMask.DATETIME):
            self.auxinfo_last_milliseconds = to_milliseconds(self.read_growing_datetime(self.auxinfo_last_milliseconds))

        if available(availability_mask, AuxInfoDataMask.ASK_TOTAL):
            self.auxinfo_last_ask_total = self.read_relative(self.auxinfo_last_ask_total)

        if available(availability_mask, AuxInfoDataMask.BID_TOTAL):
            self.auxinfo_last_bid_total = self.read_relative(self.auxinfo_last_bid_total)

        if available(availability_mask, AuxInfoDataMask.OI):
            self.auxinfo_last_oi = self.read_relative(self.auxinfo_last_oi)

        if available(availability_mask, AuxInfoDataMask.PRICE):
            self.auxinfo_last_price = self.read_relative(self.auxinfo_last_price)

        if available(availability_mask, AuxInfoDataMask.SESSION_INFO):
            self.auxinfo_last_hi_limit  = self.read_leb128()
            self.auxinfo_last_low_limit = self.read_leb128()
            self.auxinfo_last_deposit   = self.read_double()

        if available(availability_mask, AuxInfoDataMask.RATE):
            self.auxinfo_last_rate = self.read_double()

        if available(availability_mask, AuxInfoDataMask.MESSAGE):
            message = self.read_string()
        else:
            message = ""

        return AuxInfoEntry(to_datetime(self.auxinfo_last_milliseconds), self.auxinfo_last_price, self.auxinfo_last_ask_total, self.auxinfo_last_bid_total, self.auxinfo_last_oi, self.auxinfo_last_hi_limit, self.auxinfo_last_low_limit, self.auxinfo_last_deposit, self.auxinfo_last_rate, message)

    def read_own_orders_data(self):
        """Reads own orders data from file"""
        availability_mask = self.read_byte()

        if available(availability_mask, OwnOrderDataMask.DROP_ALL):
            return None
        
        order_type = OwnOrder.Type.NONE

        if available(availability_mask, OwnOrderDataMask.ACTIVE):
            if available(availability_mask, OwnOrderDataMask.STOP):
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

        return OwnTrade(to_datetime(self.own_trades_last_milliseconds), self.own_trades_last_trade_id, self.own_trades_last_order_id, self.own_trades_last_price, volume)
