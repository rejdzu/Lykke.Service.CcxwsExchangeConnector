// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const azure = require('azure-storage');
const path = require('path');
const LogFactory = require('./utils/logFactory');

class AzureTablePublisher {
    constructor(tableService, settings) {
        this._tableService = tableService;
        this._log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel);

        this._tableName = settings.Storage.TableName;
        this._tableService.createTableIfNotExists(this._tableName, (error, result, response) => {
            if (!error) {
                // result contains true if created; false if already exists
                this._log.debug(`Azure result: ${result}, response:${response}.`)
            } else {
                this._log.warn(`Azure: ${error}, result: ${result}, response:${response}.`)
            }
        });
    }

    async publishBidAsk(tick) {
        const entGen = azure.TableUtilities.entityGenerator;
        const entity = {
            PartitionKey: entGen.String(tick.asset),
            RowKey: entGen.String(tick.source + "_" + tick.timestamp),
            OriginalTimestamp: entGen.DateTime(tick.timestamp),
            Bid: entGen.String(tick.bid),
            Ask: entGen.String(tick.ask)
        };

        return new Promise((resolve, reject) => {
            this._tableService.insertEntity(this._tableName, entity, (error, result, response) => {
                if (!error) {
                    // result contains the ETag for the new entity
                    resolve(result);
                } else {
                    reject(error);
                }
            });
        });
    }

    async publishTrade(trade) {
        // TODO (Alessio): Probably write to a different table
    }
}

module.exports = AzureTablePublisher;