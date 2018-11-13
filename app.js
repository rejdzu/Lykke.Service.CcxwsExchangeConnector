const ccxws = require("ccxws");
const ccxt = require('ccxt');
const express = require('express')
const moment = require('moment');
const getRabbitMqChannel = require('./RabbitMq/rabbitMq')
const getSettings = require('./Settings/settings')
const mapping = require('./Utils/symbolMapping')
const exchangesMapping = require('./Utils/exchangesMapping')
const packageJson = require('./package.json')
const SortedMap = require("sorted-map");

process.on('uncaughtException',  e => { console.log(e) /*process.exit(1)*/ })
process.on('unhandledRejection', e => { console.log(e) /*process.exit(1)*/ })

let settings
let channel

const exchanges = [];
const exchangesOrderBooks = new SortedMap();

// There are 4 form of order books:
// 1. CCXWS
// 2. CCXT
// 3. Internal (optimized for quick update, in 'exchangesOrderBooks' variable)
// 4. Publishing to queue common format

(async function main() {
    settings = await getSettings()
    channel = await getRabbitMqChannel(settings)

    produceExchangesData()

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


async function produceExchangesData() {
    const exchanges = settings.CcxwsExchangeConnector.Main.Exchanges
    const symbols = settings.CcxwsExchangeConnector.Main.Symbols

    await Promise.all(exchanges.map (exchangeName =>
        produceExchangeData(exchangeName, symbols)
    ))
}

async function produceExchangeData(exchangeName, symbols){

    const exchange = new ccxt[exchangeName]()
    const exchange_ws = exchangesMapping.MapExchangeCcxtToCcxws(exchangeName)

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

    exchange_ws.on("l2snapshot", orderBook => updateOrderBook(orderBook, null))
    exchange_ws.on("l2update", update => updateOrderBook(null, update))

    availableMarkets.forEach(market => {
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



function updateOrderBook(orderBook, update)
{
    if (update === null) {
        const key = getKey(orderBook)
        const ob = mapToInternal(orderBook)
        exchangesOrderBooks.set(key, ob)
        return
    }

    const key = getKey(update);
    orderBook = exchangesOrderBooks.get(key)

    if (!orderBook) {
        console.log('OrderBook ' + key + ' is not found.')
        return
    } 

    update.asks.forEach(ask => {
        const updateAskPrice = parseFloat(ask.price)
        const updateAskSize = parseFloat(ask.size)

        orderBook.asks.del(updateAskPrice)

        if (updateAskSize !== 0)
            orderBook.asks.set(updateAskPrice, updateAskSize)
    });

    update.bids.forEach(bid => {
        const updateBidPrice = parseFloat(bid.price)
        const updateBidSize = parseFloat(bid.size)

        orderBook.bids.del(updateBidPrice)

        if (updateBidSize !== 0)
            orderBook.bids.set(updateBidPrice, updateBidSize)
    });

    orderBook.timestamp = moment.utc()

    const mappedOrderBook = mapToPublishing(orderBook)
    publishOrderBook(mappedOrderBook)
    publishTickPrice(mappedOrderBook)
}

function getKey(orderBook) {
    return orderBook.exchange.toLowerCase() + "_" + orderBook.marketId
}

function mapToInternal(orderBook) {
    const asks = new SortedMap();
    orderBook.asks.forEach(ask => {
        const askPrice = parseFloat(ask.price)
        const askSize = parseFloat(ask.size)

        asks.set(askPrice, askSize)
    })

    const bids = new SortedMap();
    orderBook.bids.forEach(bid => {
        const bidPrice = parseFloat(bid.price)
        const bidSize = parseFloat(bid.size)

        bids.set(bidPrice, bidSize)
    })

    const result = {}
    result.source = orderBook.exchange
    result.assetPair = orderBook.marketId
    result.asks = asks
    result.bids = bids
    // Some exchanges don't have a timestamp, as an example - Poloniex, shall be investigated
    result.timestamp = moment.utc()
    
    return result
}

function mapToPublishing(orderBook){
    
    const exchange = exchanges.find(ex => { 
        return ex.name.toLowerCase() === orderBook.source.toLowerCase()
    })
    const symbol = mapping.TryToMapSymbolBackward(orderBook.assetPair, exchange, settings)

    const base = symbol.substring(0, symbol.indexOf('/'))
    const quote = symbol.substring(symbol.indexOf("/") + 1)
    const suffixConfig = settings.CcxwsExchangeConnector.Main.ExchangesNamesSuffix
    const suffix = suffixConfig ? suffixConfig : "(w)"
    const source = exchange.name.replace(exchange.version, "").trim()
    const orderBookObj = {}
    orderBookObj.source = source + suffix
    orderBookObj.asset = symbol.replace("/", "")
    orderBookObj.assetPair = { 'base': base, 'quote': quote }
    orderBookObj.timestamp = orderBook.timestamp.toISOString()

    const descOrderedBidsPrices = Array.from(orderBook.bids.keys())
                                       .sort(function(a, b) { return b-a; })
    const bids = []
    for(let price of descOrderedBidsPrices) {
        if (price == 0)
            continue
        let size = orderBook.bids.get(price)
        if (size == 0)
            continue

        price = toFixed(price)
        size = toFixed(size)

        bids.push({ 'price': price, 'volume': size })
    }
    orderBookObj.bids = bids

    const ascOrderedAsksPrices = Array.from(orderBook.asks.keys())
                                       .sort(function(a, b) { return a-b; })
    const asks = []
    for(let price of ascOrderedAsksPrices) {
        if (price == 0)
            continue
        let size = orderBook.asks.get(price)
        if (size == 0)
            continue

        price = toFixed(price)
        size = toFixed(size)

        asks.push({ 'price': price, 'volume': size })
    }
    orderBookObj.asks = asks

    return orderBookObj
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

    log("TP: %s %s %s, bid[0]: %s, ask[0]: %s", moment().format("DD.MM.YYYY hh:mm:ss"), tickPrice.source, tickPrice.asset, tickPrice.bid, tickPrice.ask)
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

function mapOrderBookToTickPrice(orderBook) {
    const tickPrice = {}
    tickPrice.source = orderBook.source
    tickPrice.asset = orderBook.asset
    tickPrice.timestamp = orderBook.timestamp
    const bestBid = orderBook.bids.length ? orderBook.bids[0] : undefined
    const bestAsk = orderBook.asks.length ? orderBook.asks[0] : undefined
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