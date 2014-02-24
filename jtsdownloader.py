import threading
import logging
import pytz
import argparse
import sys
import tzlocal
import os
from time import sleep
from datetime import datetime, timedelta
from swigibpy import EWrapper, EPosixClientSocket, Contract


###### ENUMS

class TickType:
    unknown = 0
    bid = 1
    ask = 2
    last = 4
    high = 6
    low = 7
    close = 9

class ConnectionState:
    disconnected = 0
    broken = 1
    connected = 2

class ErrorCode:
    duplicate_orderid = 103
    cannot_find_order = 135
    historical_data_error = 162
    no_security_def_found = 200
    order_error = 201 # rejected order, cannot cancel filled order etc
    order_canceled = 202 # by tws client, for example
    error_validating_request = 321
    clientid_in_use = 326
    cross_order_repriced = 399
    order_held_locating_shares = 404
    already_connected = 501
    connection_error = 509
    connection_lost = 1100
    connection_restored = 1102
    account_data_unsubscribed = 2100
    modifying_order_while_being_modified = 2102
    md_connection_broken = 2103
    md_connection_ok = 2104
    md_connection_inactive = 2108
    order_outside_market_hours = 2109



def contract_to_string(contract):
    s = '{}-'.format(contract.symbol)
    exchange = contract.exchange
    if contract.primaryExchange:
        exchange = contract.primaryExchange
    s += exchange
    if contract.expiry:
        s += '_{}'.format(contract.expiry)
    return s


# def utc_to_local(utc_dt, local_tz):
#     local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
#     return local_tz.normalize(local_dt)  # .normalize might be unnecessary

def extract_hours(sessionstr, tz):

        splitted = sessionstr.split(";")

        first_start = None
        first_end = None
        second_start = None
        second_end = None

        #  example string when closed: '20140222:CLOSED;20140224:0930-1600'

        if not "CLOSED" in splitted[0]:

            str = splitted[0]

            strp = str[0:8] + str[9:13]
            first_start = datetime.strptime(strp, "%Y%m%d%H%M")
            first_start = tz.localize(first_start)

            strp = str[0:8] + str[14:18]
            first_end = datetime.strptime(strp, "%Y%m%d%H%M")
            first_end = tz.localize(first_end)

        if not "CLOSED" in splitted[1]:

            str = splitted[1]

            strp = str[0:8] + str[9:13]
            second_start = datetime.strptime(strp, "%Y%m%d%H%M")
            second_start = tz.localize(second_start)

            strp = str[0:8] + str[14:18]
            second_end = datetime.strptime(strp, "%Y%m%d%H%M")
            second_end = tz.localize(second_end)

        return first_start, first_end, second_start, second_end



api_started = threading.Event()
contract_details_received = threading.Event()
historical_data_received = threading.Event()
num_batches_received = 0
num_requests = None

tws = None
clientid = 5

contract = Contract()
output_file = None

prev_rth_start = None
prev_rth_end = None
next_rth_start = None
next_rth_end = None
prev_session_start = None
prev_session_end = None
next_session_start = None
next_session_end = None
contract_tz = None

last_time = None
prev_last_time = None
period = None
barsize = None
datatype = None
rth_only = None
use_pacing = None
zerobased = None

# cannot make two identical requests in 15 sec period
cooldowntime = 15

line_buffer = []
last_line = ""
dt_format = None


class MyCallbacks(EWrapper):

    def error(self, id, errCode, errString):
        global clientid
        global tws
        global connection_state
        global use_pacing
        global last_time
        global cooldowntime

        if errCode == ErrorCode.clientid_in_use:
            logging.info("Client ID {} in use, reconnecting ...".format(clientid))
            clientid += 1
            tws = EPosixClientSocket(self)
            tws.eConnect("", 7496, clientid)
        elif errCode == ErrorCode.md_connection_ok:
            logging.info("IB[{}]: {}".format(errCode, errString))
            api_started.set()
        # TODO: use a better string here!
        elif errCode == ErrorCode.historical_data_error and "Historical data request pacing violation" in errString:
            logging.info("Historical data pacing violation: retrying last batch and start using 10 sec interval between data requests...")
            logging.info(errString)
            use_pacing = True
            dt = prev_last_time.strftime("%Y%m%d %H:%M:%S")
            logging.info("Cooling down for {} seconds...".format(cooldowntime))
            sleep(cooldowntime)
            cooldowntime += 15 #  sometimes we just need to cool down for a longer time
            tws.reqHistoricalData(0, contract, dt, duration, barsize, datatype, rth_only, 1)
        elif errCode == ErrorCode.historical_data_error and "invalid step" in errString:
            logging.info("IB[{}]: {}".format(errCode, errString))
            historical_data_received.set()
        elif errCode == ErrorCode.historical_data_error and "HMDS query returned no data" in errString:
            logging.info("IB[{}]: {}".format(errCode, errString))
            historical_data_received.set()
        # requesting historical data from period too long time ago
        elif errCode == ErrorCode.error_validating_request and "Historical data queries on this contract requesting any data earlier than" in errString:
            dt = prev_last_time.strftime(dt_format)
            logging.info("IB cannot provide data from period ending {}, it's too far back in the history.".format(dt))
            historical_data_received.set()

        elif errCode == ErrorCode.connection_lost:
            # TODO: some logic to retry after connection has been momentarily lost, and eventually give up...
            logging.info("Connection lost, saving data end aborting...")
            if not output_file:
                sys.exit(2)
            historical_data_received.set()
        elif errCode == ErrorCode.no_security_def_found:
            logging.info("IB[{}]: {}".format(errCode, errString))
            if not output_file:
                sys.exit(2)
            historical_data_received.set()
        else:
            s = "IB[{}]: {}".format(errCode, errString)
            if id > -1:
                s += " (ID: {})".format(id)
            logging.info(s)


    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId,
        parentId, lastFilledPrice, clientId, whyHeld):
        pass

    def openOrder(self, orderId, contract, order, os):
        pass

    def openOrderEnd(self):
        pass

    def execDetails(self, id, contract, execution):
        pass

    def commissionReport(self, report):
        pass

    def nextValidId(self, id):
        pass

    def managedAccounts(self, openOrderEnd):
        pass

    def contractDetailsEnd(self, reqId):
        pass




    def contractDetails(self, id, cd):
        global contract_details_received
        global prev_rth_start
        global prev_rth_end
        global next_rth_start
        global next_rth_end
        global prev_session_start
        global prev_session_end
        global next_session_start
        global next_session_end
        global contract_tz

        tz = None

        if cd.timeZoneId == 'EST':
            tz = pytz.timezone('US/Eastern')

        if tz:

            prev_rth_start, prev_rth_end, next_rth_start, next_rth_end = extract_hours(cd.liquidHours, tz)
            prev_session_start, prev_session_end, next_session_start, next_session_end = extract_hours(cd.tradingHours, tz)

            if prev_session_end:
                contract_tz = prev_session_end.tzinfo
            elif next_session_end:
                contract_tz = next_session_end.tzinfo
            else:
                raise Exception("We should have at least one session defined")

            logging.debug("RTH: {} - {}, TH: {} - {}".format(prev_rth_start, prev_rth_end, prev_session_start, prev_session_end))


        else:
            logging.error("Contract timezone cannot be determined.")
            sys.exit(2)

        contract_details_received.set()

    def tickPrice(self, id, tickType, price, canAutoExecute):
        pass

    def tickSize(self, id, tickType, size):
        pass

    def tickString(self, id, tickType, genericTicks):
        pass

    def tickGeneric(self, id, tickType, value):
        pass

    def updateAccountValue(self, key, value, currency, accountName):
        pass

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        pass

    def updateAccountTime(self, timeStamp):
        pass

    def accountDownloadEnd(self, accountName):
        pass

    def historicalData(self, id, date, open, high, low, close, volume, barCount, WAP, hasGaps):
        global prev_last_time
        global last_time
        global num_batches_received
        global line_buffer
        global last_line

        local_tz = tzlocal.get_localzone()

        # special string will be printed on date field when finished
        if "finished" in date:
            num_batches_received += 1
            s = "Batch {} finished ({} lines). (msg: {})".format(num_batches_received, len(line_buffer), date)
            if num_requests > 1 and use_pacing and num_batches_received != num_requests:
                s += " {} seconds remaining".format((num_requests - num_batches_received) * 10)
            logging.info(s)

            line_buffer.reverse()

            # may happen on some circumstances, batches overlap each other by 1 bar
            if line_buffer[0] == last_line:
                del line_buffer[0]

            for line in line_buffer:
                output_file.write(line)

            last_line = line_buffer[-1]

            line_buffer.clear()

            if num_batches_received == num_requests:
                historical_data_received.set()
                return

            if use_pacing:
                sleep(10)

            # date argument is showing strange datetime-range (tws bug?) so it cannot be used to optimize batch sizing!

            dt_contract = last_time.astimezone(contract_tz)
            dt = last_time.strftime("%Y%m%d %H:%M:%S")

            logging.info("Requesting historical data batch ending {}".format(dt_contract.strftime(dt_format)))
            tws.reqHistoricalData(0, contract, dt, duration, barsize, datatype, rth_only, 1)
            prev_last_time = last_time
            last_time = None
            return

        dt = datetime.strptime(date, dt_format)
        dt = local_tz.localize(dt)

        if not last_time:
            last_time = dt

        dt = dt.astimezone(contract_tz)

        if zerobased:
            # TODO: implement this, first bar of the day starts from time zero etc ...
            pass

        dtstr = dt.strftime(dt_format)

        if datatype == "TRADES":
            line_buffer.append("{date}, {open}, {high}, {low}, {close}, {volume}, {barCount}, {WAP}, {hasGaps}\n".format(date=dtstr, open=open, high=high, low=low, close=close, volume=volume, barCount=barCount, WAP=WAP, hasGaps=hasGaps))
        # open = average bid, high = average ask
        elif datatype == "BID_ASK":
            line_buffer.append("{date}, {bid}, {ask}, {hasGaps}\n".format(date=dtstr, bid=open, ask=high, hasGaps=hasGaps))
        elif datatype == "BID" or datatype == "ASK" or datatype == "MIDPOINT" or datatype == "OPTION_IMPLIED_VOLATILITY":
            line_buffer.append("{date}, {open}, {high}, {low}, {close}, {hasGaps}\n".format(date=dtstr, open=open, high=high, low=low, close=close, hasGaps=hasGaps))
        elif datatype == "HISTORICAL_VOLATILITY":
            line_buffer.append("{date}, {vola}, {hasGaps}".format(date=dtstr, vola=open, hasGaps=hasGaps))

        logging.debug(line_buffer[-1])
        # output_file.write(s)


if __name__ == '__main__':
    # global contract
    # global clientid
    # global num_batches_received
    # global tws
    # global period
    # global barsize
    # global datatype
    # global rth_only
    # global num_requests



    parser = argparse.ArgumentParser(description="Downloads historical data from TWS")

    # optional arguments
    parser.add_argument("-e", default="now", help="ending date (in contract's timezone), format: YYYYMMDD [HH:mm:ss] (time is optional), other possible values include: end, now")
    parser.add_argument("-n", type=int, default=1, help="how many data requests to send?")
    parser.add_argument("-o", help="output filename")
    parser.add_argument("-p", default="1 min", help='periodicity of bars, for example "1 min"')
    parser.add_argument("-t", default="TRADES", choices=["TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK", "HISTORICAL_VOLATILITY", "OPTION_IMPLIED_VOLATILITY"], help="what kind of data to fetch?")
    parser.add_argument("-rth", action="store_true", help="fetch regular trading hours only")
    parser.add_argument("--pacing", action="store_true", help="pace requests 10 second apart from each other?")
    parser.add_argument("-z", action="store_true", help="use zero-based time from the beginning of trading session")
    parser.add_argument("-v", action="store_true", help="verbose mode")

    # contract arguments
    con_group = parser.add_argument_group('contract arguments')
    con_group.add_argument("-sym", help="symbol of the contract")
    con_group.add_argument("-exc", help="exchange of the contract")
    con_group.add_argument("-st", help="security type of the contract")
    con_group.add_argument("-cur", help="currency of the contract")
    con_group.add_argument("-id", type=int, help="id of the contract")
    con_group.add_argument("-exp", help="expiry (YYYYMM[DD]) of the contract")
    con_group.add_argument("-pex", help="primary exchange of the contract")
    con_group.add_argument("-str", help="strike price of the option contract")
    con_group.add_argument("-rt", choices=["C", "P"], help="right of the option contract (C/P)")
    con_group.add_argument("-mult", help="multiplier of the contract")

    args = parser.parse_args()

    if args.v:
        logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%j-%H:%M:%S')
    else:
        logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%j-%H:%M:%S')

    logging.debug(args)

    if args.sym: contract.symbol = args.sym
    if args.exc: contract.exchange = args.exc
    if args.st: contract.secType = args.st
    if args.cur: contract.currency = args.cur
    if args.id: contract.conId = args.id
    if args.exp: contract.expiry = args.exp
    if args.pex: contract.primaryExchange = args.pex
    if args.str: contract.strike = args.str
    if args.rt: contract.right = args.rt
    if args.mult: contract.multiplier = args.mult

    barsize = args.p
    datatype = args.t
    rth_only = args.rth
    use_pacing = args.pacing
    num_requests = args.n
    zerobased = args.z

    if barsize == "1 day":
        dt_format = "%Y%m%d"
    else:
        dt_format = "%Y%m%d %H:%M:%S"

    logging.info("Starting to download {}, series: {}, bartype: '{}'".format(contract_to_string(contract), datatype, barsize))

    max_duration = {
        '1 secs': '60 S',
        '5 secs': '7200 S',
        '15 secs': '14400 S',
        '30 secs': '1 D',
        '1 min': '2 D',
        '2 mins': '2 D',
        '3 mins': '2 D',
        '15 mins': '1 W',
        '30 mins': '1 W',
        '1 hour': '1 M',
        '1 day': '1 Y'
    }

    duration = max_duration[barsize]

    # need to get extra info from tws in order to proceed

    callbacks = MyCallbacks()

    tws = EPosixClientSocket(callbacks)

    tws.eConnect("", 7496, clientid)

    api_started.wait()

    logging.info("API functional, getting started...")

    logging.info("Requesting contract details...")
    tws.reqContractDetails(0, contract)
    contract_details_received.wait(5)
    if not prev_session_end and not next_session_end:
        logging.info("Failed to retrieve contract details. Aborting...")
        sys.exit(2)
    logging.info("Contract details received.")

    # historical data is requested and received in local timezone

    now = datetime.now()
    local_tz = tzlocal.get_localzone()
    now = local_tz.localize(now)

    if args.e == "now":
        if not next_session_start:
            if args.rth:
                endtime = prev_rth_end.astimezone(local_tz)
            else:
                endtime = prev_session_end.astimezone(local_tz)
        else:
            if rth_only:
                if now > next_rth_start.astimezone(local_tz):
                    endtime = now
                else:
                    endtime = datetime.combine(datetime.date(now), next_rth_end.time())
            else:
                if now > next_session_start.astimezone(local_tz):
                    endtime = now
                else:
                    endtime = datetime.combine(datetime.date(now), next_session_end.time())
            endtime = contract_tz.localize(endtime)
            endtime = endtime.astimezone(local_tz)
    elif args.e == "end":
        if not prev_session_end:
            if args.rth:
                endtime = next_rth_end.astimezone(local_tz)
            else:
                endtime = next_session_end.astimezone(local_tz)
        else:
            if args.rth:
                endtime = prev_rth_end.astimezone(local_tz)
            else:
                endtime = prev_session_end.astimezone(local_tz)
    else:
        try:
            endtime = datetime.strptime(args.e, "%Y%m%d")
            if args.rth:
                if prev_rth_end:
                    endtime = datetime.combine(datetime.date(endtime), prev_rth_end.time())
                else:
                    endtime = datetime.combine(datetime.date(endtime), next_rth_end.time())
            else:
                if prev_rth_end:
                    endtime = datetime.combine(datetime.date(endtime), prev_session_end.time())
                else:
                    endtime = datetime.combine(datetime.date(endtime), next_session_end.time())
            endtime = contract_tz.localize(endtime)
            endtime = endtime.astimezone(local_tz)
        except ValueError:
            endtime = datetime.strptime(args.e, "%Y%m%d %H:%M:%S")
            endtime = contract_tz.localize(endtime)
            endtime = endtime.astimezone(local_tz)
        except ValueError:
            print("end must be in format: %Y%m%d or %Y%m%d %H:%M:%S")
            sys.exit(2)


    # we got all the necessary information now

    if args.o:
        filename = args.o
    else:
        filename = "{}_{}_{}.csv".format(contract_to_string(contract), barsize.replace(" ", ""), datatype)
    output_file = open(filename, 'w')


    s = "Receiving {} batches of historical data...".format(num_requests)
    if num_requests > 1 and use_pacing:
        s += " {} seconds remaining".format((num_requests - num_batches_received) * 10)
    logging.info(s)
    prev_last_time = endtime
    tws.reqHistoricalData(0, contract, endtime.strftime("%Y%m%d %H:%M:%S"), duration, barsize, datatype, rth_only, 1)
    historical_data_received.wait()


    if output_file.tell() > 0: #  file not empty
        logging.info("Reversing the output file...")

        output_file.close()

        with open(filename, 'r') as input_file:
            lines = input_file.readlines()

        lines.reverse()
        with open(filename, 'w') as output_file:
            for line in lines:
                output_file.write(line)
    else:
        output_file.close()
        logging.info("Nothing was written to the output file, removing output file.")
        os.remove(filename)


    logging.info("Disconnecting...")
    tws.eDisconnect()