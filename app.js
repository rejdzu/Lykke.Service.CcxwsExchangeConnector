const ccxws = require("ccxws");
const ccxt = require('ccxt');
const express = require('express')
const moment = require('moment');
const sortedMap = require("sorted-map");
const semaphore = require("semaphore");
const getRabbitMqChannel = require('./RabbitMq/rabbitMq')
const getSettings = require('./Settings/settings')
const mapping = require('./Utils/symbolMapping')
const exchangesMapping = require('./Utils/exchangesMapping')
const packageJson = require('./package.json')

process.on('uncaughtException',  e => { console.log(e) /*process.exit(1)*/ })
process.on('unhandledRejection', e => { console.log(e) /*process.exit(1)*/ })

let settings
let channel

const exchanges = [];
const _sync = semaphore(1);
const internalOrderBooksMap = new sortedMap();

// There are 4 form of order books:
// 1. CCXWS
// 2. CCXT
// 3. Internal (optimized for quick update - with sorted map for asks and bids, stored in the 'internalOrderBooksMap' variable)
// 4. Publishing to queue common contract format

(async function main() {
    settings = await getSettings()
    channel = await getRabbitMqChannel(settings)

    subscribeToExchangesData()

    startWebServer()
})();


function startWebServer() {
    const response = {
        "Name": "Lykke.Service.CcxwsExchangeConnector",
        "Version": packageJson.version,
        "Env": null,
        "IsDebug": false,
        "IssueIndicators": []
      }
      
    const app = express()

    app.get('/api/IsAlive', function (req, res) {
        res.header("Content-Type",'application/json')
        res.send(JSON.stringify(response, null, 4))
    })
    
    app.get('/settings', async function (req, res) {
        res.header("Content-Type",'application/json')
        res.send(JSON.stringify(settings, null, 4))
    })

    const server = app.listen(5000, function () {
       let host = server.address().address
       const port = server.address().port

       if (host === "::") { 
           host = "localhost" }
       console.log("Listening at http://%s:%s", host, port)
    })
}


async function subscribeToExchangesData() {
    const exchanges = settings.CcxwsExchangeConnector.Main.Exchanges
    const symbols = settings.CcxwsExchangeConnector.Main.Symbols

    await Promise.all(exchanges.map (exchangeName =>
        subscribeToExchangeData(exchangeName, symbols)
    ))
}

async function subscribeToExchangeData(exchangeName, symbols) {

    const exchange = new ccxt[exchangeName]()
    const exchange_ws = exchangesMapping.MapExchangeCcxtToCcxws(exchangeName)
    exchange_ws.reconnectIntervalMs = settings.CcxwsExchangeConnector.Main.ReconnectIntervalMs

    try{
        exchange.timeout = 30 * 1000
        await exchange.loadMarkets()
        exchanges.push(exchange)
    } catch (e) {
        console.log(exchange.id + " can't load markets: " + e)
        return
    }

    const availableMarkets = getAvailableMarketsForExchange(exchange, symbols)
    if (availableMarkets.length === 0) {
        console.log(exchange.id + " doesn't have any symbols from config.")
        return
    }

    exchange_ws.on("l2snapshot", orderBook =>
        {
            try {
                _sync.take(function() {
                    l2snapshotEventHandle(orderBook)
                    _sync.leave()
                })
            }
            catch(e) {
                console.log('Exception: ' + e)
            }
        }
    )
    exchange_ws.on("l2update", updateOrderBook =>
        {
            try {
                _sync.take(function() {
                    l2updateEventHandle(updateOrderBook)
                    _sync.leave()
                })
            }
            catch(e) {
                console.log('Exception: ' + e)
            }
        }
    )

    availableMarkets.forEach(market => {
        if (exchange_ws.hasLevel2Snapshots)
            exchange_ws.subscribeLevel2Snapshots(market)
        else
            exchange_ws.subscribeLevel2Updates(market)
    });
}

function getAvailableMarketsForExchange(exchange, symbols) {
    const result = []
    
    for (const symbol of symbols) {
        let market = exchange.findMarket(mapping.MapAssetForward(symbol, settings))
        // Inversed - first trying to map, then try to use original
        // Example:
        // in cofig symbols = BTC/USD, mapping = USD -> USDT
        // logic: first try to find BTC/USDT, then BTC/USD
        // because of Poloniex which ccxt shows has BTC/USD,
        // but it doesn't work, BTC/USDT works.
        const exchangeHasMapped = typeof market === "object"
        if (exchangeHasMapped) {
            result.push(market)
        } else {
            market = exchange.findMarket(symbol)
            const exchangeHas = typeof market === "object"
            if (exchangeHas) {
                result.push(market)
            }
        }
    }

    return result
}

function l2snapshotEventHandle(orderBook)
{
    const key = getKey(orderBook)
    const internalOrderBook = mapCcxwsToInternal(orderBook)
    
    internalOrderBooksMap.set(key, internalOrderBook)
    
    const publishingOrderBook = mapInternalToPublishing(internalOrderBook)
    publishOrderBook(publishingOrderBook)
    publishTickPrice(publishingOrderBook)
}

function l2updateEventHandle(updateOrderBook)
{
    const key = getKey(updateOrderBook);

    let internalOrderBook = internalOrderBooksMap.get(key)

    if (!internalOrderBook) {
        console.log('OrderBook ' + key + ' is not found.')
        return
    } 

    updateOrderBook.asks.forEach(ask => {
        const updateAskPrice = parseFloat(ask.price)
        const updateAskSize = parseFloat(ask.size)

        internalOrderBook.asks.del(updateAskPrice)
        
        if (updateAskSize !== 0)
            internalOrderBook.asks.set(updateAskPrice, updateAskSize)
    });

    updateOrderBook.bids.forEach(bid => {
        const updateBidPrice = parseFloat(bid.price)
        const updateBidSize = parseFloat(bid.size)

        internalOrderBook.bids.del(updateBidPrice)

        if (updateBidSize !== 0)
            internalOrderBook.bids.set(updateBidPrice, updateBidSize)
    });

    internalOrderBook.timestamp = moment.utc()

    const publishingOrderBook = mapInternalToPublishing(internalOrderBook)
    publishOrderBook(publishingOrderBook)
    publishTickPrice(publishingOrderBook)
}

function getKey(ccxwsOrderBook) {
    return ccxwsOrderBook.exchange.toLowerCase() + "_" + ccxwsOrderBook.marketId
}

function mapCcxwsToInternal(ccxwsOrderBook) {
    const asks = new sortedMap();
    ccxwsOrderBook.asks.forEach(ask => {
        const askPrice = parseFloat(ask.price)
        const askSize = parseFloat(ask.size)

        asks.set(askPrice, askSize)
    })

    const bids = new sortedMap();
    ccxwsOrderBook.bids.forEach(bid => {
        const bidPrice = parseFloat(bid.price)
        const bidSize = parseFloat(bid.size)

        bids.set(bidPrice, bidSize)
    })

    const internalOrderBook = {}
    internalOrderBook.source = ccxwsOrderBook.exchange
    internalOrderBook.assetPair = ccxwsOrderBook.marketId
    internalOrderBook.asks = asks
    internalOrderBook.bids = bids
    // Some exchanges don't have a timestamp, as an example - Poloniex, shall be investigated
    internalOrderBook.timestamp = moment.utc()
    
    return internalOrderBook
}

function mapInternalToPublishing(internalOrderBook){
    
    const exchange = exchanges.find(ex => { 
        return ex.name.toLowerCase() === internalOrderBook.source.toLowerCase()
    })
    const symbol = mapping.TryToMapSymbolBackward(internalOrderBook.assetPair, exchange, settings)

    const base = symbol.substring(0, symbol.indexOf('/'))
    const quote = symbol.substring(symbol.indexOf("/") + 1)
    const suffixConfig = settings.CcxwsExchangeConnector.Main.ExchangesNamesSuffix
    const suffix = suffixConfig ? suffixConfig : "(w)"
    const source = exchange.name.replace(exchange.version, "").trim()
    const publishingOrderBook = {}
    publishingOrderBook.source = source + suffix
    publishingOrderBook.asset = symbol.replace("/", "")
    publishingOrderBook.assetPair = { 'base': base, 'quote': quote }
    publishingOrderBook.timestamp = internalOrderBook.timestamp.toISOString()

    const descOrderedBidsPrices = Array.from(internalOrderBook.bids.keys())
                                       .sort(function(a, b) { return b-a; })
    const bids = []
    for(let price of descOrderedBidsPrices) {
        if (price == 0)
            continue
        let size = internalOrderBook.bids.get(price)
        if (size == 0)
            continue

        price = toFixed(price)
        size = toFixed(size)

        bids.push({ 'price': price, 'volume': size })
    }
    publishingOrderBook.bids = bids

    const ascOrderedAsksPrices = Array.from(internalOrderBook.asks.keys())
                                       .sort(function(a, b) { return a-b; })
    const asks = []
    for(let price of ascOrderedAsksPrices) {
        if (price == 0)
            continue
        let size = internalOrderBook.asks.get(price)
        if (size == 0)
            continue

        price = toFixed(price)
        size = toFixed(size)

        asks.push({ 'price': price, 'volume': size })
    }
    publishingOrderBook.asks = asks

    return publishingOrderBook
}

// TODO: next methods must be refactored
function publishOrderBook(orderBook) {
    
    sendToRabitMQ(settings.CcxwsExchangeConnector.RabbitMq.OrderBooks, orderBook)

    log("OB: %s %s %s, bids[0]: %s, asks[0]: %s", moment().format("DD.MM.YYYY hh:mm:ss"), orderBook.source, orderBook.asset, orderBook.bids[0].price, orderBook.asks[0].price)
}

function publishTickPrice(orderBook) {
    const tickPrice = mapOrderBookToTickPrice(orderBook)
    if (!tickPrice) {
        return
    }

    sendToRabitMQ(settings.CcxwsExchangeConnector.RabbitMq.TickPrices, tickPrice)

    log("TP: %s %s %s, bids[0]: %s, asks[0]: %s", moment().format("DD.MM.YYYY hh:mm:ss"), tickPrice.source, tickPrice.asset, tickPrice.bid, tickPrice.ask)
}

function sendToRabitMQ(rabbitExchange, object) {
    //TODO: check if it is changed, if not - don't publish (settings in config)

    const objectJson = JSON.stringify(object)

    try{
        if (channel)
            channel.publish(rabbitExchange, '', new Buffer(objectJson))
    }
    catch(e){
        console.log("Error while sending a message to rabbit: " + e)
    }
}

function mapOrderBookToTickPrice(publishingOrderBook) {
    const tickPrice = {}
    tickPrice.source = publishingOrderBook.source
    tickPrice.asset = publishingOrderBook.asset
    tickPrice.timestamp = publishingOrderBook.timestamp
    const bestBid = publishingOrderBook.bids.length ? publishingOrderBook.bids[0] : undefined
    const bestAsk = publishingOrderBook.asks.length ? publishingOrderBook.asks[0] : undefined
    if (!bestBid || !bestAsk) {
        return null
    }
    if (bestBid && bestBid.price) {
        tickPrice.bid = bestBid.price
    }
    else {
        tickPrice.bid = null
    }
    if (bestAsk && bestAsk.price) {
        tickPrice.ask = bestAsk.price
    }
    else {
        tickPrice.ask = null
    }

    return tickPrice
}

function log(...args) {
    if (settings.CcxwsExchangeConnector.Main.Verbose) {
        console.log.apply(this, args)
    }
}

function toFixed(number) {
    return number.toFixed(8).replace(/\.?0+$/,"")
}