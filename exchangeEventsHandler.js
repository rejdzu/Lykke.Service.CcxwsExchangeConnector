const moment = require('moment');
const sortedMap = require("sorted-map");
const path = require('path');
const LogFactory =  require('./utils/logFactory')
const mapping = require('./utils/assetPairsMapping')

class ExchangeEventsHandler {
    
    constructor(exchange, settings, rabbitMq) {
        this._exchange = exchange
        this._settings = settings
        this._rabbitMq = rabbitMq
        this._orderBooks = new sortedMap()
        this._lastTimePublished = new sortedMap()
        this._log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel)
    }

    async l2snapshotEventHandle(orderBook) {
        const key = orderBook.marketId
        const internalOrderBook = this._mapCcxwsToInternal(orderBook)
        
        this._orderBooks.set(key, internalOrderBook)

        const lastTimePublished = this._lastTimePublished.get(key)
        if (!lastTimePublished || moment() - lastTimePublished > this._settings.Main.PublishingIntervalMs)
        {
            const publishingOrderBook = this._mapInternalToPublishing(internalOrderBook)
            await this._publishOrderBook(publishingOrderBook)
            await this._publishTickPrice(publishingOrderBook)

            this._lastTimePublished.set(key, moment.utc())
        }
    }

    async l2updateEventHandle(updateOrderBook) {
        const key = updateOrderBook.marketId

        const internalOrderBook = this._orderBooks.get(key)

        if (!internalOrderBook) {
            this._log.info(`Internal order book ${this._exchange.name} ${key} is not found during update.`)
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

        const lastTimePublished = this._lastTimePublished.get(key)
        if (!lastTimePublished || moment() - lastTimePublished > 1000)
        {
            const publishingOrderBook = this._mapInternalToPublishing(internalOrderBook)
            await this._publishOrderBook(publishingOrderBook)
            await this._publishTickPrice(publishingOrderBook)

            this._lastTimePublished.set(key, moment.utc())
        }
    }

    _mapCcxwsToInternal(ccxwsOrderBook) {
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
    
    _mapInternalToPublishing(internalOrderBook) {
        
        const symbol = mapping.MapAssetPairBackward(internalOrderBook.assetPair, this._settings)
    
        const base = symbol.substring(0, symbol.indexOf('/'))
        const quote = symbol.substring(symbol.indexOf("/") + 1)
        const suffixConfig = this._settings.Main.ExchangesNamesSuffix
        const suffix = suffixConfig ? suffixConfig : "(w)"
        const source = this._exchange.name.replace(this._exchange.version, "").trim()
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
    
            price = this._toFixed(price)
            size = this._toFixed(size)
    
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
    
            price = this._toFixed(price)
            size = this._toFixed(size)
    
            asks.push({ 'price': price, 'volume': size })
        }
        publishingOrderBook.asks = asks
    
        return publishingOrderBook
    }

    async _publishOrderBook(orderBook) {
        
        await this._rabbitMq.send(this._settings.RabbitMq.OrderBooks, orderBook)
    
        this._log.debug(`OB: ${orderBook.source} ${orderBook.asset}, bids:${orderBook.bids.length}, asks:${orderBook.asks.length}, best bid:${orderBook.bids[0].price}, best ask:${orderBook.asks[0].price}.`)
    }
    
    async _publishTickPrice(orderBook) {
        const tickPrice = this._mapOrderBookToTickPrice(orderBook)
        if (!tickPrice) {
            return
        }
    
        await this._rabbitMq.send(this._settings.RabbitMq.TickPrices, tickPrice)

        this._log.debug(`TP: ${tickPrice.source} ${tickPrice.asset}, bid: ${tickPrice.bid}, ask:${tickPrice.ask}.`)
    }
    
    _mapOrderBookToTickPrice(publishingOrderBook) {
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
    
    _toFixed(number) {
        return number.toFixed(8).replace(/\.?0+$/,"")
    }
}

module.exports = ExchangeEventsHandler