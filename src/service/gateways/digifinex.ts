/// <reference path="../utils.ts" />
/// <reference path="../../common/models.ts" />
/// <reference path="nullgw.ts" />
///<reference path="../config.ts"/>
///<reference path="../utils.ts"/>
///<reference path="../interfaces.ts"/>

import Q = require("q");
import md5 = require("md5")
import request = require("request");
import url = require("url");
import querystring = require("querystring");
import Config = require("../config");
import NullGateway = require("./nullgw");
import Models = require("../../common/models");
import Utils = require("../utils");
import util = require("util");
import Interfaces = require("../interfaces");
import moment = require("moment");
import _ = require("lodash");
import log from "../logging";
var shortId = require("shortid");
var Deque = require("collections/deque");

interface DigifinexMarketTrade {
    date: number;
    price: string;
    amount: string;
    type: string;
}

interface DigifinexResponse<T> {
    code: number,
    data: T
}

type DigifinexMarketLevel = [
    number, // price
    number // size amount
]

interface DigifinexOrderBook {
    code: number,
    date: number,
    asks: DigifinexMarketLevel[];
    bids: DigifinexMarketLevel[];
}

function decodeSide(side: string) {
    switch (side) {
        case "buy": return Models.Side.Bid;
        case "sell": return Models.Side.Ask;
        default: return Models.Side.Unknown;
    }
}

function encodeSide(side: Models.Side) {
    switch (side) {
        case Models.Side.Bid: return "buy";
        case Models.Side.Ask: return "sell";
        default: return "";
    }
}

function encodeTimeInForce(tif: Models.TimeInForce, type: Models.OrderType) {
    if (type === Models.OrderType.Market) {
        return "exchange market";
    }
    else if (type === Models.OrderType.Limit) {
        if (tif === Models.TimeInForce.FOK) return "exchange fill-or-kill";
        if (tif === Models.TimeInForce.GTC) return "exchange limit";
    }
    throw new Error("unsupported tif " + Models.TimeInForce[tif] + " and order type " + Models.OrderType[type]);
}

class DigifinexMarketDataGateway implements Interfaces.IMarketDataGateway {
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    private _since: number = null;
    MarketTrade = new Utils.Evt<Models.GatewayMarketTrade>();
    private onTrades = ({data: trades}: Models.Timestamped<DigifinexResponse<DigifinexMarketTrade[]>>) => {
        _.forEach(trades, trade => {
            const px = trade.price;
            var sz = trade.amount;
            var time = moment.unix(trade.date).toDate();
            var side = decodeSide(trade.type);
            var mt = new Models.GatewayMarketTrade(px, sz, time, this._since === null, side);
            this.MarketTrade.trigger(mt);
        });

        this._since = moment().unix();
    };
    private downloadMarketTrades = () => {
        var qs = {
            symbol: this._symbolProvider.symbol,
            timestamp: this._since === null ? moment.utc().unix() : this._since
        };
        this._http
            .get<DigifinexResponse<DigifinexMarketTrade[]>>("trade_detail", qs)
            .then(this.onTrades)
            .done();
    };

    private static ConvertToMarketSide([price, size]: DigifinexMarketLevel): Models.MarketSide {
        return new Models.MarketSide(price, size);
    }

    private static ConvertToMarketSides(level: DigifinexMarketLevel[]): Models.MarketSide[] {
        return _.map(level, DigifinexMarketDataGateway.ConvertToMarketSide);
    }

    MarketData = new Utils.Evt<Models.Market>();
    private onMarketData = (book: Models.Timestamped<DigifinexOrderBook>) => {
        const bids = DigifinexMarketDataGateway.ConvertToMarketSides(book.data.bids);
        const asks = DigifinexMarketDataGateway.ConvertToMarketSides(book.data.asks);
        this.MarketData.trigger(new Models.Market(bids, asks, book.time));
    };

    private downloadMarketData = () => {
        const queryBody = {
            symbol: this._symbolProvider.symbol,
            timestamp: moment.utc().unix()
        }
        this._http
            .get<DigifinexOrderBook>("depth", queryBody)
            .then(this.onMarketData)
            .done();
    };

    // TODO: Calculate best interval
    constructor(
        timeProvider: Utils.ITimeProvider,
        private _http: DigifinexHttp,
        private _symbolProvider: DigifinexSymbolProvider) {

        timeProvider.setInterval(this.downloadMarketData, moment.duration(5, "seconds"));
        timeProvider.setInterval(this.downloadMarketTrades, moment.duration(15, "seconds"));

        this.downloadMarketData();
        this.downloadMarketTrades();

        _http.ConnectChanged.on(s => this.ConnectChanged.trigger(s));
    }
}

interface RejectableResponse {
    message: string;
}

// TODO: Not updated
interface DigifinexNewOrderRequest {
    symbol: string;
    amount: string;
    price: string; //Price to buy or sell at. Must be positive. Use random number for market orders.
    exchange: string; //always "Digifinex"
    side: string; // buy or sell
    type: string; // "market" / "limit" / "stop" / "traling-stop" / "fill-or-kill" / "exchange market" / "exchange limit" / "exchange stop" / "exchange trailing-stop" / "exchange fill-or-kill". (type starting by "exchange " are exchange orders, others are margin trading orders)
    is_hidden?: boolean;
}

interface DigifinexNewOrderResponse extends RejectableResponse {
    order_id: string;
}

interface DigifinexCancelOrderRequest {
    order_id: string;
}

interface DigifinexCancelReplaceOrderRequest extends DigifinexNewOrderRequest {
    order_id: string;
}

interface DigifinexCancelReplaceOrderResponse extends DigifinexCancelOrderRequest, RejectableResponse { }

interface DigifinexOrderStatusRequest {
    order_id: string;
}

interface DigifinexMyTradesRequest {
    symbol: string;
    timestamp: number;
}

interface DigifinexMyTradesResponse extends RejectableResponse {
    price: string;
    amount: string;
    timestamp: number;
    exchange: string;
    type: string;
    fee_currency: string;
    fee_amount: string;
    tid: number;
    order_id: string;
}

interface DigifinexOrderStatusResponse extends RejectableResponse {
    symbol: string;
    exchange: string; // bitstamp or Digifinex
    price: number;
    avg_execution_price: string;
    side: string;
    type: string; // "market" / "limit" / "stop" / "trailing-stop".
    timestamp: number;
    is_live: boolean;
    is_cancelled: boolean;
    is_hidden: boolean;
    was_forced: boolean;
    executed_amount: string;
    remaining_amount: string;
    original_amount: string;
}

class DigifinexOrderEntryGateway implements Interfaces.IOrderEntryGateway {
    OrderUpdate = new Utils.Evt<Models.OrderStatusUpdate>();
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    supportsCancelAllOpenOrders = () : boolean => { return false; };
    cancelAllOpenOrders = () : Q.Promise<number> => { return Q(0); };

    generateClientOrderId = () => shortId.generate();

    public cancelsByClientOrderId = false;

    private convertToOrderRequest = (order: Models.OrderStatusReport): DigifinexNewOrderRequest => {
        return {
            amount: order.quantity.toString(),
            exchange: "Digifinex",
            price: order.price.toString(),
            side: encodeSide(order.side),
            symbol: this._symbolProvider.symbol,
            type: encodeTimeInForce(order.timeInForce, order.type)
        };
    }

    sendOrder = (order: Models.OrderStatusReport) => {
        var req = this.convertToOrderRequest(order);

        this._http
            .post<DigifinexNewOrderRequest, DigifinexNewOrderResponse>("order/new", req)
            .then(resp => {
                if (typeof resp.data.message !== "undefined") {
                    this.OrderUpdate.trigger({
                        orderStatus: Models.OrderStatus.Rejected,
                        orderId: order.orderId,
                        rejectMessage: resp.data.message,
                        time: resp.time
                    });
                    return;
                }

                this.OrderUpdate.trigger({
                    orderId: order.orderId,
                    exchangeId: resp.data.order_id,
                    time: resp.time,
                    orderStatus: Models.OrderStatus.Working
                });
            }).done();

        this.OrderUpdate.trigger({
            orderId: order.orderId,
            computationalLatency: Utils.fastDiff(new Date(), order.time)
        });
    };

    cancelOrder = (cancel: Models.OrderStatusReport) => {
        var req = { order_id: cancel.exchangeId };
        this._http
            .post<DigifinexCancelOrderRequest, any>("order/cancel", req)
            .then(resp => {
                if (typeof resp.data.message !== "undefined") {
                    this.OrderUpdate.trigger({
                        orderStatus: Models.OrderStatus.Rejected,
                        cancelRejected: true,
                        orderId: cancel.orderId,
                        rejectMessage: resp.data.message,
                        time: resp.time
                    });
                    return;
                }

                this.OrderUpdate.trigger({
                    orderId: cancel.orderId,
                    time: resp.time,
                    orderStatus: Models.OrderStatus.Cancelled
                });
            })
            .done();

        this.OrderUpdate.trigger({
            orderId: cancel.orderId,
            computationalLatency: Utils.fastDiff(new Date(), cancel.time)
        });
    };

    replaceOrder = (replace: Models.OrderStatusReport) => {
        this.cancelOrder(replace);
        this.sendOrder(replace);
    };

    private downloadOrderStatuses = () => {
        var tradesReq = { timestamp: this._since.unix(), symbol: this._symbolProvider.symbol };
        this._http
            .post<DigifinexMyTradesRequest, DigifinexMyTradesResponse[]>("mytrades", tradesReq)
            .then(resps => {
                _.forEach(resps.data, t => {

                    this._http
                        .post<DigifinexOrderStatusRequest, DigifinexOrderStatusResponse>("order/status", { order_id: t.order_id })
                        .then(r => {

                            this.OrderUpdate.trigger({
                                exchangeId: t.order_id,
                                lastPrice: parseFloat(t.price),
                                lastQuantity: parseFloat(t.amount),
                                orderStatus: DigifinexOrderEntryGateway.GetOrderStatus(r.data),
                                averagePrice: parseFloat(r.data.avg_execution_price),
                                leavesQuantity: parseFloat(r.data.remaining_amount),
                                cumQuantity: parseFloat(r.data.executed_amount),
                                quantity: parseFloat(r.data.original_amount)
                            });

                        })
                        .done();
                });
            }).done();

        this._since = moment.utc();
    };

    private static GetOrderStatus(r: DigifinexOrderStatusResponse) {
        if (r.is_cancelled) return Models.OrderStatus.Cancelled;
        if (r.is_live) return Models.OrderStatus.Working;
        if (r.executed_amount === r.original_amount) return Models.OrderStatus.Complete;
        return Models.OrderStatus.Other;
    }

    private _since = moment.utc();
    private _log = log("tribeca:gateway:DigifinexOE");
    constructor(
        timeProvider: Utils.ITimeProvider,
        private _details: DigifinexBaseGateway,
        private _http: DigifinexHttp,
        private _symbolProvider: DigifinexSymbolProvider) {

        _http.ConnectChanged.on(s => this.ConnectChanged.trigger(s));
        timeProvider.setInterval(this.downloadOrderStatuses, moment.duration(8, "seconds"));
    }
}

// OK
class DigifinexRateLimitMonitor {
    private _log = log("tribeca:gateway:rlm");
    
    private _queue = Deque();
    private _durationMs: number;

    public add = () => {
        var now = moment.utc();

        while (now.diff(this._queue.peek()) > this._durationMs) {
            this._queue.shift();
        }

        this._queue.push(now);

        if (this._queue.length > this._max_req) {
            this._log.error(`Exceeded ${this._method} rate limit`, { nRequests: this._queue.length, max: this._max_req, durationMs: this._durationMs });
            throw('limit-rate-reached');
        }
    }

    constructor(private _max_req: number, duration: moment.Duration, private _method: string) {
        this._durationMs = duration.asMilliseconds();
    }
}

const generateSignature = <Object>(body, apiKey, apiSecret): Object => {
    const sortedValues = _(body)
            // Apend api key and api secret to body    
            .set('apiKey', apiKey)
            .set('apiSecret', apiSecret)
            // Sort object by key params, alphabetically
            .toPairs()
            .sortBy(0)
            .fromPairs()
            // Retrieve the value of the fields in a joined string
            .values()
            .join('');

        const signature = md5(sortedValues);

        return _(body)
            .set('apiKey', this._apiKey)
            .set('sign', signature);
}

class DigifinexHttp {
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    private _timeout = 15000;

    get = <T>(actionUrl: string, qs?: any): Q.Promise<Models.Timestamped<T>> => {
        const url = this._baseUrl + "/" + actionUrl;
        var opts = {
            timeout: this._timeout,
            url: url,
            qs: this.appendSignature(qs) || undefined,
            method: "GET"
        };

        return this.doRequest<T>(opts, url);
    };
    
    // Digifinex seems to have a race condition where nonces are processed out of order when rapidly placing orders
    // Retry here - look to mitigate in the future by batching orders?
    post = <TRequest, TResponse>(actionUrl: string, msg: TRequest): Q.Promise<Models.Timestamped<TResponse>> => {
        return this.postOnce<TRequest, TResponse>(actionUrl, _.clone(msg)).then(resp => {
            var rejectMsg: string = (<any>(resp.data)).message;
            if (typeof rejectMsg !== "undefined" && rejectMsg.indexOf("Nonce is too small") > -1)
                return this.post<TRequest, TResponse>(actionUrl, _.clone(msg));
            else
                return resp;
        });
    }
    private appendSignature = <Object>(body): Object => generateSignature(body, this._apiKey, this._secret)

    private postOnce = <TRequest, TResponse>(actionUrl: string, msg: TRequest): Q.Promise<Models.Timestamped<TResponse>> => {
        const url = this._baseUrl + "/" + actionUrl;
        const opts: request.Options = {
            timeout: this._timeout,
            url: url,
            body: this.appendSignature(msg),
            json: true,
            method: "POST"
        };

        return this.doRequest<TResponse>(opts, url);
    };

    private doRequest = <TResponse>(msg: request.Options, url: string): Q.Promise<Models.Timestamped<TResponse>> => {
        var d = Q.defer<Models.Timestamped<TResponse>>();
        switch(msg.method) {
            case "POST":
                this._post_monitor.add();
                break;
            case "GET":
                this._get_monitor.add();
                break;
            default:
                break;
        }
        request(msg, (err, resp, body) => {
            if (err) {
                this._log.error(err, "Error returned: url=", url, "err=", err);
                d.reject(err);
            }
            else {
                try {
                    const t = new Date();
                    const data = JSON.parse(body);
                    data.message = data.code ? this.parseDigifinexCode(data.code) : this.parseDigifinexCode(-1);
                    d.resolve(new Models.Timestamped(data, t));
                }
                catch (err) {
                    this._log.error(err, "Error parsing JSON url=", url, "err=", err, ", body=", body);
                    d.reject(err);
                }
            }
        });

        return d.promise;
    };

    private parseDigifinexCode = (code: number) : string => {
        switch(code) {
            case 0:	return 'Success';
            case 10002: return 'Invalid ApiKey';
            case 10003: return 'Sign doesn\'t match';
            case 10004: return 'Illegal request parameters';
            case 10005: return 'Request frequency exceeds the limit';
            case 10006: return 'Unauthorized to execute this request';
            case 10007: return 'IP address Unauthorized';
            case 10008: return 'Timestamp for this request is invalid';
            case 20001: return 'Trade is not open for this trading pair';
            case 20002: return 'Trade of this trading pair is suspended';
            case 20003: return 'Invalid price or amount';
            case 20004: return 'Price exceeds daily limit';
            case 20005: return 'Price exceeds down limit';
            case 20006: return 'Cash Amount is less than 10CNY';
            case 20007: return 'Price precision error';
            case 20008: return 'Amount precision error';
            case 20009: return 'Amount is less than the minimum requirement';
            case 20010: return 'Cash Amount is less than the minimum requirement';
            case 20011: return 'Insufficient balance';
            case 20012: return 'Invalid trade type (valid value: buy/sell)';
            case 20013: return 'No such order';
            case 20014: return 'Invalid date (Valid format: 2018-07-25)';
            case 20015: return 'Dates exceed the limit';
            default: return 'Unknown digifinex code';
        }
    }

    private _log = log("tribeca:gateway:DigifinexHTTP");
    private _baseUrl: string;
    private _apiKey: string;
    private _secret: string;
    private _nonce: number;

    constructor(config: Config.IConfigProvider, private _get_monitor: DigifinexRateLimitMonitor, private _post_monitor: DigifinexRateLimitMonitor) {
        this._baseUrl = config.GetString("DigifinexHttpUrl")
        this._apiKey = config.GetString("DigifinexKey");
        this._secret = config.GetString("DigifinexSecret");

        this._nonce = new Date().valueOf();
        this._log.info("Starting nonce: ", this._nonce);
        setTimeout(() => this.ConnectChanged.trigger(Models.ConnectivityStatus.Connected), 10);
    }
}

type DigifinexCoinBalance = {
    [key: string]: number
}

interface DigifinexPositionResponse {
    code: number;
    date: number;
    free: DigifinexCoinBalance;
    frozen: DigifinexCoinBalance;
}

class DigifinexPositionGateway implements Interfaces.IPositionGateway {
    PositionUpdate = new Utils.Evt<Models.CurrencyPosition>();

    private onRefreshPositions = () => {
        const body = { timestamp: moment().utc().unix() }
        this._http.post<object, DigifinexPositionResponse>("myposition", body)
        .then(res => {
            const symbols = _.keys(res.data.free);
            _.forEach(symbols, symbol => {
                const frozen = res.data.frozen[symbol];
                const available = res.data.free[symbol];
                const amt = parseFloat(frozen + available);
                const cur = Models.toCurrency(symbol);
                const held = frozen;
                const rpt = new Models.CurrencyPosition(amt, held, cur);
                this.PositionUpdate.trigger(rpt);
            });
        })
        .done();
    }

    private _log = log("tribeca:gateway:DigifinexPG");
    constructor(timeProvider: Utils.ITimeProvider, private _http: DigifinexHttp) {
        timeProvider.setInterval(this.onRefreshPositions, moment.duration(15, "seconds"));
        this.onRefreshPositions();
    }
}
// OK
class DigifinexBaseGateway implements Interfaces.IExchangeDetailsGateway {
    public get hasSelfTradePrevention() {
        return false;
    }

    name(): string {
        return "Digifinex";
    }

    makeFee(): number {
        return 0.002;
    }

    takeFee(): number {
        return 0.002;
    }

    exchange(): Models.Exchange {
        return Models.Exchange.Digifinex;
    }

    constructor(public minTickIncrement: number) {} 
}

class DigifinexSymbolProvider {
    public symbol: string;

    constructor(pair: Models.CurrencyPair) {
        this.symbol = `${Models.fromCurrency(pair.base).toLowerCase()}_${Models.fromCurrency(pair.quote).toLowerCase()}`;
    }
}

class Digifinex extends Interfaces.CombinedGateway {
    constructor(timeProvider: Utils.ITimeProvider, config: Config.IConfigProvider, symbol: DigifinexSymbolProvider, pricePrecision: number) {
        // Request Rate limiters for POST and GET http methods
        const post_monitor = new DigifinexRateLimitMonitor(60, moment.duration(1, "minutes"), 'POST');
        const get_monitor = new DigifinexRateLimitMonitor(180, moment.duration(1, "minutes"), 'GET');
        const http = new DigifinexHttp(config, get_monitor, post_monitor);
        const details = new DigifinexBaseGateway(pricePrecision);
        
        const orderGateway = config.GetString("DigifinexOrderDestination") == "Digifinex"
            ? <Interfaces.IOrderEntryGateway>new DigifinexOrderEntryGateway(timeProvider, details, http, symbol)
            : new NullGateway.NullOrderGateway();

        super(
            new DigifinexMarketDataGateway(timeProvider, http, symbol),
            orderGateway,
            new DigifinexPositionGateway(timeProvider, http),
            details
        );
    }
}

// OK
export async function createDigifinex(timeProvider: Utils.ITimeProvider, config: Config.IConfigProvider, pair: Models.CurrencyPair) : Promise<Interfaces.CombinedGateway> {
    const apiSecret = config.GetString("DigifinexSecret");
    const apiKey = config.GetString("DigifinexKey");
    const qsParams = {
        timestamp: moment.utc().unix()
    }
    const qsBody = generateSignature(qsParams, apiKey, apiSecret)

    const detailsUrl = `${config.GetString("DigifinexHttpUrl")}/trade_pairs`;
    const {data: response} = await request.get({url: detailsUrl, qs: qsBody, json: true});
    const symbolList = _.keys(response.data);

    const symbol = new DigifinexSymbolProvider(pair);    

    if (_.includes(symbolList, symbol.symbol) == true) {
        const [ , price_precision, , ] = response.data.data[symbol.symbol];
        return new Digifinex(timeProvider, config, symbol, 10**(-1*price_precision));
    }

    throw new Error("cannot match pair to a Digifinex Symbol " + pair.toString());
}


