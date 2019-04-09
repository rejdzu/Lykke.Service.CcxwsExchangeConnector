const ccxws = require("ccxws");

function MapExchangeCcxtToCcxws(ccxtExchangeName){
    switch(ccxtExchangeName) {
        case 'binance':
            return new ccxws.Binance()
        case 'bitfinex':
            return new ccxws.Bitfinex()
        case 'bitflyer':
            return new ccxws.Bitflyer()
        case 'bitmex':
            return new ccxws.BitMEX()
        case 'bitstamp':
            return new ccxws.Bitstamp()
        case 'bittrex':
            return new ccxws.Bittrex()
        case 'coinex':
            return new ccxws.coinex()
        case 'gdax':
            return new ccxws.GDAX()
        case 'gemini':
            return new ccxws.Gemini()
        case 'hitbtc':
            return new ccxws.HitBTC()
        case 'huobi':
            return new ccxws.Huobi()
        case 'kraken':
            return new ccxws.Kraken()
        case 'okex':
            return new ccxws.OKEx()
        case 'poloniex':
            return new ccxws.Poloniex()
        case 'zb':
            return new ccxws.zb()
        default:
            throw "This point can't be reached."
    }
}

module.exports.MapExchangeCcxtToCcxws = MapExchangeCcxtToCcxws
