const ccxws = require("ccxws")
const ccxt = require('ccxt')
const express = require('express')
const path = require('path')
const LogFactory =  require('./utils/logFactory')
const getSettings = require('./settings/settings')
const assetPairsMapping = require('./utils/assetPairsMapping')
const exchangesMapping = require('./utils/exchangesMapping')
const packageJson = require('./package.json')
const exchangeEventsHandler = require('./exchangeEventsHandler')
const net = require('net');
const azure = require('azure-storage')
const crypto = require('crypto')
const SocketPublisher = require('./SocketPublisher')
const AzureTablePublisher = require('./AzureTablePublisher')
const FakeExchangeClient = require('./utils/fakeExchangeClient/fakeExchangeClient')
const ReconnectingSocket = require('./ReconnectingSocket');

let settings
let log

(async function main() {    
    settings = (await getSettings()).CcxwsExchangeConnector
    log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel)

    process.on('uncaughtException',  e => log.warn(`Unhandled error: ${e}, ${e.stack}.`))
    process.on('unhandledRejection', e => log.warn(`Unhandled error: ${e}, ${e.stack}.`))

    // const socket = await setupSocketConnection();
    const socket = new ReconnectingSocket(
        settings.Sanitizer.Port,
        settings.Sanitizer.Host,
        settings.Sanitizer.ReconnectionIntervalMs,
        settings.Sanitizer.TimeoutMs,
        settings.Main.LoggingLevel
    );
    await socket.setup();

    let publishers = [
        new SocketPublisher(socket, settings),
        //new AzureTablePublisher(azure.createTableService(settings.Storage.ConnectionString), settings)
    ];

    if(settings.FakeExchanges && settings.FakeExchanges.UseFakeExchanges) {
        subscribeToFakeExchangesData(publishers)
    } else {
        subscribeToExchangesData(publishers)
    }

    startWebServer()
})();

// function setupSocketConnection() {
//     const port = settings.Sanitizer.Port;
//     const host = settings.Sanitizer.Host;
//
//     const socket = new net.Socket();
//
//     // const connectSocket = () => socket.connect(port, host);
//     const connectSocket = () => new Promise((resolve, reject) => {
//
//         socket.on("error", (err) => {
//             if (err.code === 'EALREADY') {
//                 log.info(`Got error but already reconnecting, skipping`);
//                 return;
//             }
//             log.warn(`Got error (${err.code}): ${err}. Trying to reconnect to sanitizer.`);
//             setTimeout(async () => await connectSocket(), settings.Sanitizer.ReconnectionIntervalMs);
//         });
//         socket.on("timeout", (err) => {
//             // socket.destroy();
//             log.info(`Trying to reconnect to sanitizer in ${settings.Sanitizer.ReconnectionIntervalMs}ms`);
//             setTimeout(async () => await connectSocket(), settings.Sanitizer.ReconnectionIntervalMs);
//         });
//
//         socket.on("close", (err) => {
//             console.log("close")
//         });
//
//         socket.on("connect", () => {
//             log.info('Sanitizer connected on: ' + port + ':' + host);
//
//             socket.setTimeout(1000);
//             resolve(socket);
//         });
//
//         socket.connect(port, host)
//     });
//     // socket.once("connect", () => log.info('Sanitizer connected on: ' + port + ':' + host));
//
//     //
//     // return new Promise((resolve, reject) => {
//     //     socket.connect(port, host, () => {
//     //         log.info('Sanitizer connected on: ' + port + ':' + host);
//     //         resolve(socket);
//     //     });
//     // });
//
//     return connectSocket();
// }

async function subscribeToFakeExchangesData(publishers) {
    const exchanges = generateRandomExchanges()
    const symbols = generateRandomSymbols()

    await Promise.all(exchanges.map (exchangeName =>
        subscribeToFakeExchangeData(exchangeName, symbols, publishers)
    ))
}

async function subscribeToFakeExchangeData(exchangeName, symbols, publishers) {
    const exchange = {
        name: exchangeName,
        version: 1
    }

    const exchange_ws = new FakeExchangeClient(exchange.name)
    const availableMarkets = symbols.map(x => { 
        const split = x.split('/')
        return { id: x, base: split[0], quote: split[1] }
     })

    const handler = new exchangeEventsHandler(exchange, publishers, settings)

    exchange_ws.on("l2snapshot", async orderBook => await handler.l2snapshotEventHandle(orderBook))
    exchange_ws.on("l2update", async updateOrderBook => await handler.l2updateEventHandle(updateOrderBook))
    exchange_ws.on("trade", async trade => await handler.tradeEventHandler(trade))

    availableMarkets.forEach(market => {
        // Exchanges that provide the initial snapshot are unreliable with the updates endpoint
        // For those exchanges, we connect to the full snapshot endpoint and consume that directly
        // so we are sure to keep a complete view of the order book

        if (exchange_ws.hasLevel2Snapshots){
            exchange_ws.subscribeLevel2Snapshots(market)
        } else {
            exchange_ws.subscribeLevel2Updates(market)
        }

        exchange_ws.subscribeTrades(market)
    });
}

async function subscribeToExchangesData(publishers) {
    const exchanges = settings.Main.Exchanges
    const symbols = settings.Main.Symbols

    await Promise.all(exchanges.map (exchangeName =>
        subscribeToExchangeData(exchangeName, symbols, publishers)
    ))
}

async function subscribeToExchangeData(exchangeName, symbols, publishers) {
    const exchange = new ccxt[exchangeName]()
    // const exchange_ws = new ccxws[exchangeName]()
    const exchange_ws = exchangesMapping.MapExchangeCcxtToCcxws(exchangeName)
    exchange_ws.reconnectIntervalMs = settings.Main.ReconnectIntervalMs

    try {
        exchange.timeout = 30 * 1000
        await exchange.loadMarkets()
    } catch (e) {
        log.warn(`${exchange.id} can't load markets: ${e}`)
        return
    }

    const availableMarkets = getAvailableMarketsForExchange(exchange, symbols)
    if (availableMarkets.length === 0) {
        log.warn(`${exchange.id} doesn't have any symbols from config.`)
        return
    }

    const handler = new exchangeEventsHandler(exchange, publishers, settings)

    exchange_ws.on("l2snapshot", async orderBook => await handler.l2snapshotEventHandle(orderBook))
    exchange_ws.on("l2update", async updateOrderBook => await handler.l2updateEventHandle(updateOrderBook))
    exchange_ws.on("trade", async trade => await handler.tradeEventHandler(trade))

    availableMarkets.forEach(market => {
        // Exchanges that provide the initial snapshot are unreliable with the updates endpoint
        // For those exchanges, we connect to the full snapshot endpoint and consume that directly
        // so we are sure to keep a complete view of the order book

        if (exchange_ws.hasLevel2Snapshots){
            exchange_ws.subscribeLevel2Snapshots(market)
        } else {
            exchange_ws.subscribeLevel2Updates(market)
        }

        exchange.fetchTrades(market.symbol, undefined)
            .then(async (trades) => {
                if (trades.length <= 0) {
                    return
                }

                let latestTrade = trades.sort((a, b) => b.timestamp - a.timestamp)[0];
                log.info(`Got latest trade for ${market.id} from ${exchangeName}`);
                await handler.ccxtTradeEventHandler(latestTrade);
            });

        exchange_ws.subscribeTrades(market)
    });
}

function generateRandomExchanges() {
    const exchangesCount = settings.FakeExchanges.ExchangesCount
    const exchanges = new Set()
    
    while(exchanges.size < exchangesCount) {
        const value = randomString(10, 'abcdefghijklmnopqrstuwxyz')
        exchanges.add(value);
    }

    return Array.from(exchanges)
}

function generateRandomSymbols() {
    const symbolsCount = settings.FakeExchanges.SymbolsCount
    const symbols = new Set()
    
    while(symbols.size < symbolsCount) {
        const value1 = randomString(3, 'ABCDEFGHIJKLMNOPQRSTUWXYZ')
        const value2 = randomString(3, 'ABCDEFGHIJKLMNOPQRSTUWXYZ')
        symbols.add(value1 + "/" + value2);
    }

    return Array.from(symbols)
}

function randomString (howMany, chars) {
    chars = chars || 'abcdefghijklmnopqrstuwxyzABCDEFGHIJKLMNOPQRSTUWXYZ0123456789';
    const rnd = crypto.randomBytes(howMany)
    const value = new Array(howMany)
    const len = Math.min(256, chars.length)
    const d = 256 / len

    for (var i = 0; i < howMany; i++) {
        value[i] = chars[Math.floor(rnd[i] / d)]
    };

    return value.join('');
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

    app.get('/api/isAlive', function (req, res) {
        res.header("Content-Type",'application/json')
        res.send(JSON.stringify(response, null, 4))
    })
    
    app.get('/api/settings', async function (req, res) {
        res.header("Content-Type",'application/json')
        res.send(JSON.stringify(settings, null, 4))
    })

    const server = app.listen(5100, function () {
       let host = server.address().address
       const port = server.address().port

       if (host === "::") { 
           host = "localhost" }
        log.info(`Listening at http://${host}:${port}`)
    })
}
