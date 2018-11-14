const ccxws = require("ccxws");
const ccxt = require('ccxt');
const express = require('express')
const winston = require('winston')
const getRabbitMqChannel = require('./RabbitMq/rabbitMq')
const getSettings = require('./Settings/settings')
const assetPairsMapping = require('./Utils/assetPairsMapping')
const exchangesMapping = require('./Utils/exchangesMapping')
const packageJson = require('./package.json')
const exchangeEventsHandler = require('./exchangeEventsHandler')

process.on('uncaughtException',  e => { winston.warn(e) /*process.exit(1)*/ })
process.on('unhandledRejection', e => { winston.warn(e) /*process.exit(1)*/ })

let settings
let channel

(async function main() {
    winston.add(new winston.transports.Console({
        level: 'debug',
        handleExceptions: true,
        format: winston.format.simple(),
        colorize: true,
      }));

    winston.info('Starting...')

    settings = await getSettings()
    channel = await getRabbitMqChannel(settings)

    subscribeToExchangesData()

    startWebServer()
})();

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

    try {
        exchange.timeout = 30 * 1000
        await exchange.loadMarkets()
    } catch (e) {
        console.log(exchange.id + " can't load markets: " + e)
        return
    }

    const availableMarkets = getAvailableMarketsForExchange(exchange, symbols)
    if (availableMarkets.length === 0) {
        console.log(exchange.id + " doesn't have any symbols from config.")
        return
    }

    const handler = new exchangeEventsHandler(exchange, settings, channel)

    exchange_ws.on("l2snapshot", orderBook =>
        //{
        //     try {
        //         _sync.take(function() {
                    handler.l2snapshotEventHandle(orderBook)
        //             _sync.leave()
        //         })
        //     }
        //     catch(e) {
        //         console.log('Exception: ' + e)
        //     }
        //}
    )
    exchange_ws.on("l2update", updateOrderBook =>
        // {
        //     try {
        //         _sync.take(function() {
                    handler.l2updateEventHandle(updateOrderBook)
        //             _sync.leave()
        //         })
        //     }
        //     catch(e) {
        //         console.log('Exception: ' + e)
        //     }
        // }
    )

    availableMarkets.forEach(market => {
        if (exchange_ws.hasLevel2Snapshots){
            exchange_ws.subscribeLevel2Snapshots(market)
            console.log('Subscribed to snapshot ' + exchange.name + ' ' + market.symbol)
        } else {
            exchange_ws.subscribeLevel2Updates(market)
            console.log('Subscribed to update ' + exchange.name + ' ' + market.symbol)
        }
    });
}

function getAvailableMarketsForExchange(exchange, symbols) {
    const result = []
    
    for (const symbol of symbols) {
        let market = exchange.findMarket(assetPairsMapping.TryToMapAssetPairForward(symbol, exchange, settings))
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