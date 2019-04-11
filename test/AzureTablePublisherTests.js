// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const assert = require('chai').assert;
const describe = require('mocha').describe;
const sinon = require('sinon');
const AzureTablePublisher = require('../AzureTablePublisher');

const settings = {
    Main: {
        LoggingLevel: 'INFO',
    },
    Storage: {
        TableName: "marketData"
    }
};

describe('AzureTablePublisher', () => {
    const tick = {
        source: "Bitstamp(w)",
        asset: "BTCUSD",
        bid: "3900",
        ask: "4300",
        timestamp: '2019-04-11T12:03:24+00:00'
    };

    const fakeTableService = {
        createTableIfNotExists: sinon.fake(),
        insertEntity: sinon.spy()
    };

    const azureTablePublisher = new AzureTablePublisher(fakeTableService, settings);

    // Reset fakes before each test
    beforeEach(async () => {
        fakeTableService.createTableIfNotExists.resetHistory();
        fakeTableService.insertEntity.resetHistory();
    });

    describe('publishBidAsk', () => {
        it("writes an entity for the message to table storage", () => {
            azureTablePublisher.publishBidAsk(tick);

            assert(fakeTableService.insertEntity.calledOnce, "table storage should be written to exactly once");
            let expectedTableEntity = {
                PartitionKey: { _: "BTCUSD", $: "Edm.String" },
                RowKey: { _: "Bitstamp(w)_" + tick.timestamp, $: "Edm.String" },
                // TODO (Alessio): It does not make sense to test this yet because it's not fed from the exchange
                OriginalTimestamp: { _: "2019-04-11T12:03:24+00:00", $: "Edm.DateTime" },
                Bid: { _: "3900", $: "Edm.String" },
                Ask: { _: "4300", $: "Edm.String" }
            };

            const [tableName, actualEntity, callback] = fakeTableService.insertEntity.args[0];
            assert.deepEqual(settings.Storage.TableName, tableName, "The publisher should write to the correct table");
            assert.deepEqual(actualEntity, expectedTableEntity, "The entity should contain all information");
        });
    });

    describe('publishTrade', () => {
        const trade = {
            exchange: 'bitstamp',
            marketId: 'BTC/USD',
            side: 'buy',
            price: '4300',
            amount: 1
        };

        it("does not store anything", () => {
            azureTablePublisher.publishTrade(trade);

            assert(fakeTableService.insertEntity.notCalled);
        });
    });
});
