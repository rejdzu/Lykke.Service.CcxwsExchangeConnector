// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const assert = require('chai').assert;
const describe = require('mocha').describe;
const sinon = require('sinon');
const ccxt = require('ccxt');
const ExchangeEventsHandler = require('../exchangeEventsHandler');

const settings = {
    Main: {
        LoggingLevel: 'INFO',
        AssetsMapping: [],
        PublishingIntervalMs: 200
    },
    Storage: {
        TableName: "marketData"
    }
};

describe('ExchangeEventsHandler', () => {
    // Mock objects with fake handlers that we can inspect
    const publishers = [
        { publishBidAsk: sinon.spy(), publishTrade: sinon.spy() },
        { publishBidAsk: sinon.spy(), publishTrade: sinon.spy() }
    ];

    const snapshot = {
        exchange: 'bitstamp',
        marketId: 'BTC/USD',
        bids: [ { price: 4000, size: 1 }, { price: 3900, size: 1 } ],
        asks: [ { price: 4300, size: 1 }, { price: 4400, size: 1 } ]
    };
    const newSnapshot = {
        exchange: 'bitstamp',
        marketId: 'BTC/USD',
        bids: [ { price: 4010, size: 1 }, { price: 3910, size: 1 } ],
        asks: [ { price: 4310, size: 1 }, { price: 4410, size: 1 } ]
    };

    let exchange = new ccxt['bitstamp'];
    let eventHandler = new ExchangeEventsHandler(exchange, publishers, settings);

    // TODO (Alessio): At the moment we have no way of testing timestamps because they are set internally
    describe('l2snapshotEventHandle', async () => {

        it('should publish the correct tick price immediately', async () => {
            await eventHandler.l2snapshotEventHandle(snapshot);

            publishers.forEach((publisher) => {
                assert(publisher.publishBidAsk.calledOnce,
                       "The initial snapshot update should should be notified to all publishers.");

                // Get the first argument of the first call
                let actual = publisher.publishBidAsk.args[0][0];
                let expected = {
                    source: "Bitstamp(w)",
                    asset: "BTCUSD",
                    bid: "4000",
                    ask: "4300",
                    // TODO (Alessio): Currently the timestamp is populated internally and there is not way to test this
                    timestamp: actual.timestamp
                };
                assert.deepEqual(actual, expected, "All notified snapshots should contain correct values.");
            });
        });

        it('should not publish the second change if it comes within the publishing interval', async () => {
            await eventHandler.l2snapshotEventHandle(snapshot);
            await eventHandler.l2snapshotEventHandle(newSnapshot);

            assert(publishers.every(p => p.publishBidAsk.calledOnce),
                   "Only the initial snapshot should have been notified to the publishers.");
        });

        it('should publish the second change if it comes after more than the publishing interval', async () => {
            await eventHandler.l2snapshotEventHandle(snapshot);
            await sleep(settings.Main.PublishingIntervalMs);
            await eventHandler.l2snapshotEventHandle(newSnapshot);

            assert(publishers.every(p => p.publishBidAsk.calledTwice),
                   "All publishers should be notified for snapshots after the interval.");
        });
    });

    describe("l2updateEventHandle", async () => {
        let updateOrderBook = {
            exchange: 'bitstamp',
            marketId: 'BTC/USD',
            bids: [ { price: 4000, size: 0 } ],
            asks: [ { price: 4400, size: 2 } ]
        };
        let secondUpdateOrderBook = {
            exchange: 'bitstamp',
            marketId: 'BTC/USD',
            bids: [ { price: 4600, size: 3 } ],
            asks: [ { price: 4200, size: 2 } ]
        };

        // Setup the initial snapshot (otherwise updates are not recorded) and
        // reset the fake handlers before each test
        beforeEach(async () => {
            eventHandler = new ExchangeEventsHandler(exchange, publishers, settings);
            await eventHandler.l2snapshotEventHandle(snapshot);
            // TODO (Alessio): Remove this once the interval is configurable
            await sleep(1000);

            publishers.forEach(p => {
                p.publishBidAsk.resetHistory();
                p.publishTrade.resetHistory();
            });
        });

        it('should publish the correct tick price immediately if one second since the last update has passed', async () => {
            await eventHandler.l2updateEventHandle(updateOrderBook);

            publishers.forEach((publisher) => {
                assert(publisher.publishBidAsk.calledOnce,
                       "The tick update should should be notified to all publishers.");

                // Get the first argument of the first call
                let actual = publisher.publishBidAsk.args[0][0];
                let expected = {
                    source: "Bitstamp(w)",
                    asset: "BTCUSD",
                    bid: "3900",
                    ask: "4300",
                    // TODO (Alessio): Currently the timestamp is populated internally and there is not way to test this
                    timestamp: actual.timestamp
                };
                assert.deepEqual(actual, expected, "All notified updates should contain correct values.");
            });
        });

        it('should not publish the second change if it comes within the publishing interval', async () => {
            await eventHandler.l2updateEventHandle(updateOrderBook);
            await eventHandler.l2updateEventHandle(secondUpdateOrderBook);

            publishers.forEach((publisher) => {
                assert(publisher.publishBidAsk.calledOnce,
                       "Only the updates after the interval should be published.");
            });
        });

        it('should not publish the second change if it does not change the bid/ask prices', async () => {
            let nonImpactingUpdate = {
                exchange: 'bitstamp',
                marketId: 'BTC/USD',
                bids: [ { price: 3900, size: 3 } ],
                asks: [ { price: 4400, size: 2 } ]
            };
            await eventHandler.l2updateEventHandle(nonImpactingUpdate);

            publishers.forEach((publisher) => {
                assert(publisher.publishBidAsk.notCalled,
                       "Should not publish updates that do not change the bid/ask prices.");
            });
        });
    });

    describe("tradeEventHandler", async () => {
        let trade = {
            exchange: 'bitstamp',
            marketId: "BTC/USD",
            side: 'buy',
            price: "4000",
            amount: 1
        };

        it('should publish each trade message', async () => {
            await eventHandler.tradeEventHandler(trade);

            publishers.forEach((publisher) => {
                assert(publisher.publishBidAsk.notCalled,
                    "Trades should be published to all publishers.");
            });
        });
    })
});

async function sleep(msec) {
    return new Promise(resolve => setTimeout(resolve, msec));
}