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

describe('ExchangeEventsHandler', function() {
    // Mock objects with fake handlers that we can inspect
    let sanitizerSocket = {
        write: sinon.spy()
    };
    let tableService = {
        insertEntity: sinon.spy()
    };

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
    let eventHandler = new ExchangeEventsHandler(exchange, sanitizerSocket, tableService, settings);

    // TODO (Alessio): At the moment we have no way of testing timestamps because they are set internally
    describe('l2snapshotEventHandle', async () => {

        // Restore the fake handler before each test
        beforeEach(() => sinon.restore());

        it('should publish and save the correct tick price immediately', async () => {
            await eventHandler.l2snapshotEventHandle(snapshot);

            assert(sanitizerSocket.write.calledOnce,
                   "The initial snapshot update should should be written socket exactly once.");

            let actualSocketMessage = JSON.parse(sanitizerSocket.write.args[0]);
            let expectedSocketMessage = {
                source: "Bitstamp(w)",
                asset: "BTCUSD",
                bid: "4000",
                ask: "4300",
                // TODO (Alessio): Currently the timestamp is populated internally and there is not way to test this
                timestamp: actualSocketMessage.timestamp
            };
            assert.deepEqual(actualSocketMessage, expectedSocketMessage,
                         "The socket message should contain correct values.");

            assert(tableService.insertEntity.calledOnce,
                   "The initial snapshot table should be written azure table storage exactly once.");

            let [tableName, entity, callback] = tableService.insertEntity.args[0];
            assert.strictEqual(tableName, 'marketData',
                               "The snapshot update should be written to the correct azure table.");

            let actualTableEntity = entity;
            let expectedTableEntity = {
                PartitionKey: { _: "BTCUSD", $: "Edm.String" },
                RowKey: { _: "Bitstamp(w)_" + actualSocketMessage.timestamp, $: "Edm.String" },
                // TODO (Alessio): It does not make sense to test this yet because it's not fed from the exchange
                OriginalTimestamp: { _: actualSocketMessage.timestamp, $: "Edm.DateTime" },
                Bid: { _: "4000", $: "Edm.String" },
                Ask: { _: "4300", $: "Edm.String" }
            };

            assert.deepEqual(actualTableEntity, expectedTableEntity,
                              "The table entity should contain correct values");
        });

        it('should not publish the second change if it comes within the publishing interval', async () => {
            await eventHandler.l2snapshotEventHandle(snapshot);
            await eventHandler.l2snapshotEventHandle(newSnapshot);

            assert(sanitizerSocket.write.calledOnce, "Only the initial snapshot should have been written to the socket.");
            assert(tableService.insertEntity.calledOnce, "Only the initial snapshot should have been saved to table storage");
        });

        it('should publish the second change if it comes after more than the publishing interval', async () => {
            await eventHandler.l2snapshotEventHandle(snapshot);
            await sleep(settings.Main.PublishingIntervalMs);
            await eventHandler.l2snapshotEventHandle(newSnapshot);

            assert(sanitizerSocket.write.calledTwice);
            assert(tableService.insertEntity.calledTwice);
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
            eventHandler = new ExchangeEventsHandler(exchange, sanitizerSocket, tableService, settings);
            await eventHandler.l2snapshotEventHandle(snapshot);
            // TODO (Alessio): Remove this once the interval is configurable
            await sleep(1000);

            sanitizerSocket.write.resetHistory();
            tableService.insertEntity.resetHistory();
        });

        it('should publish and save the correct tick price immediately if one second since the last update has passed', async () => {
            await eventHandler.l2updateEventHandle(updateOrderBook);

            assert(sanitizerSocket.write.calledOnce,
                "The initial snapshot update should should be written socket exactly once.");

            let actualSocketMessage = JSON.parse(sanitizerSocket.write.args[0]);
            let expectedSocketMessage = {
                source: "Bitstamp(w)",
                asset: "BTCUSD",
                bid: "3900",
                ask: "4300",
                // TODO (Alessio): Currently the timestamp is populated internally and there is not way to test this
                timestamp: actualSocketMessage.timestamp
            };
            assert.deepEqual(actualSocketMessage, expectedSocketMessage,
                "The socket message should contain correct values.");

            assert(tableService.insertEntity.calledOnce,
                "The initial snapshot table should be written azure table storage exactly once.");

            let [tableName, entity, callback] = tableService.insertEntity.args[0];

            assert.strictEqual(tableName, 'marketData',
                "The snapshot update should be written to the correct azure table.");

            // TODO (Alessio): This is not really nice, find an alternative
            let actualTableEntity = entity;
            let expectedTableEntity = {
                PartitionKey: { _: "BTCUSD", $: "Edm.String" },
                RowKey: { _: "Bitstamp(w)_" + actualSocketMessage.timestamp, $: "Edm.String" },
                // TODO (Alessio): It does not make sense to test this yet because it's not fed from the exchange
                OriginalTimestamp: { _: actualSocketMessage.timestamp, $: "Edm.DateTime" },
                Bid: { _: "3900", $: "Edm.String" },
                Ask: { _: "4300", $: "Edm.String" }
            };

            assert.deepEqual(actualTableEntity, expectedTableEntity,
                "The table entity should contain correct values");
        });

        it('should not publish the second change if it comes within the publishing interval', async () => {
            await eventHandler.l2updateEventHandle(updateOrderBook);
            await eventHandler.l2updateEventHandle(secondUpdateOrderBook);

            assert(sanitizerSocket.write.calledOnce, "Only the updates after the interval should be written to the socket.");
            assert(tableService.insertEntity.calledOnce, "Only the updates after the interval should be saved to table storage");
        });

        it('should not publish the second change if it does not change the bid/ask prices', async () => {
            let nonImpactingUpdate = {
                exchange: 'bitstamp',
                marketId: 'BTC/USD',
                bids: [ { price: 3900, size: 3 } ],
                asks: [ { price: 4400, size: 2 } ]
            };
            await eventHandler.l2updateEventHandle(nonImpactingUpdate);

            assert(sanitizerSocket.write.notCalled, "Should not publish updates that do not change the bid/ask prices.");
            assert(tableService.insertEntity.notCalled, "Should not store updates that do not change the bid/ask prices.");
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

        it('should publish each trade message on the socket, without modifying it', async () => {
            await eventHandler.tradeEventHandler(trade);

            assert(sanitizerSocket.write.calledOnce, "Trades should be published to the socket unchanged");
            assert(tableService.insertEntity.notCalled, "Trades should not be stored in azure table storage");
        });
    })
});

async function sleep(msec) {
    return new Promise(resolve => setTimeout(resolve, msec));
}