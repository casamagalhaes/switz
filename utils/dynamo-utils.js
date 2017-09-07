const MAX_READ_ATTEMPTS = 10;
const MAX_WRITE_ATTEMPTS = 10;

class DynamoUtils {

    constructor(client) {
        this.client = client;
    }

    _splitifyBatchGetRequest(requestItems) {
        let result = [];
        Object.keys(requestItems).forEach(tableName => {
            let current = null;
            let count = 0;
            requestItems[tableName].Keys.forEach(key => {
                if (!current || count == 100) {
                    count = 0;
                    current = {}
                    current[tableName] = {
                        Keys: []
                    };
                    result.push(current);
                }
                current[tableName].Keys.push(key);
                count++;
            })
        });
        return result;
    }

    _splitifyBatchWriteRequest(requestItems) {
        let result = [];
        Object.keys(requestItems).forEach(tableName => {
            let current = null;
            let count = 0;
            requestItems[tableName].forEach(op => {
                if (!current || count == 25) {
                    count = 0;
                    current = {}
                    current[tableName] = [];
                    result.push(current);
                }
                current[tableName].push(op);
                count++;
            })
        });
        return result;
    }

    get(params) {
        return new Promise((resolve, reject) => {
            this.client.get(params, (err, data) => {
                if (err)
                    return reject(err);
                return resolve(data.Item);
            })
        });
    }

    delete(params) {
        return new Promise((resolve, reject) => {
            this.client.delete(params, (err, data) => {
                if (err)
                    return reject(err);
                return resolve(data.Item);
            })
        });
    }

    update(params) {
        return new Promise((resolve, reject) => {
            this.client.update(params, (err, data) => {
                if (err)
                    return reject(err);
                return resolve(data);
            })
        });
    }

    put(params) {
        return new Promise((resolve, reject) => {
            this.client.put(params, (err, data) => {
                if (err)
                    return reject(err);
                return resolve(data);
            })
        });
    }

    scan(params) {
        return new Promise((resolve, reject) => {
            this.client.scan(params, (err, data) => {
                if (err)
                    return reject(err);
                let limit = params.Limit;
                let items = data.Items;
                if (data.LastEvaluatedKey) {
                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                    this.scan(params)
                        .then(result => {
                            if (limit > 0)
                                return resolve(items.concat(result).slice(0, limit));
                            return resolve(items.concat(result));
                        })
                        .catch(err => reject(err));
                    return;
                }
                if (limit > 0)
                    return resolve(items.slice(0, limit));
                return resolve(items);
            });
        });
    }

    query(params) {
        return new Promise((resolve, reject) => {
            this.client.query(params, (err, data) => {
                if (err)
                    return reject(err);
                let limit = params.Limit;
                let items = data.Items;
                if (data.LastEvaluatedKey) {
                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                    this.query(params)
                        .then(result => {
                            if (limit > 0)
                                return resolve(items.concat(result).slice(0, limit));
                            return resolve(items.concat(result));
                        })
                        .catch(err => reject(err));
                    return;
                }
                if (limit > 0)
                    return resolve(items.slice(0, limit));
                return resolve(items);
            });
        });
    }

    batchWrite(requestItems) {

        let doBatch = (requestItems, attempts) => {
            return new Promise((resolve, reject) => {
                this.client.batchWrite({
                    RequestItems: requestItems
                }, (err, data) => {
                    if (err)
                        return reject(err);
                    if (data.UnprocessedItems) {
                        if (Object.keys(data.UnprocessedItems).length > 0) {
                            if (!attempts || attempts < 0)
                                attempts = 0;
                            if (attempts < MAX_WRITE_ATTEMPTS) {
                                setTimeout(() => {
                                    doBatch(data.UnprocessedItems, attempts + 1)
                                        .then(attemptData => {
                                            if (attemptData.UnprocessedItems) {
                                                for (let tableName in data.UnprocessedItems) {
                                                    let tableItems = attemptData.UnprocessedItems[tableName];
                                                    if (tableItems) {
                                                        data.UnprocessedItems[tableName] = data.UnprocessedItems[tableName].reduce((unprocessedItems, dataItem) => {
                                                            let found = tableItems.find(attemptDataItem => (attemptDataItem.DeleteRequest && dataItem.DeleteRequest && attemptDataItem.DeleteRequest.Key.id == dataItem.DeleteRequest.Key.id) || (attemptDataItem.PutRequest && dataItem.PutRequest && attemptDataItem.PutRequest.Item.id == dataItem.PutRequest.Item.id));
                                                            if (found)
                                                                unprocessedItems.push(dataItem);
                                                            return unprocessedItems;
                                                        }, []);
                                                    } else
                                                        delete data.UnprocessedItems[tableName];
                                                }
                                                if (data.UnprocessedItems && Object.keys(data.UnprocessedItems) == 0)
                                                    delete data.UnprocessedItems;
                                            } else
                                                delete data.UnprocessedItems;
                                            if (!data.UnprocessedItems)
                                                console.log(`[DynamoUtils.batchWrite] - lote resubmetido pela tentativa ${attempts + 2} gravado com sucesso`);
                                            return resolve(data);
                                        })
                                        .catch(err => reject(err));
                                }, Math.pow(2, attempts) * 100);
                                return;
                            } else {
                                console.log(`[DynamoUtils.batchWrite] - alguns itens não puderam ser escritos: ${JSON.stringify(data.UnprocessedItems)}`);
                                return reject({
                                    code: 'CAPACIDADE_ESCRITA_INSUFICIENTE',
                                    message: 'não foi possível escrever todos os dados'
                                });
                            }
                        } else
                            delete data.UnprocessedItems;
                    }
                    if (attempts === undefined)
                        console.log('[DynamoUtils.batchWrite] - lote submetido pela tentativa 1 gravado com sucesso');
                    return resolve(data);
                });
            });
        }

        return new Promise((resolve, reject) => {
            let ps = this._splitifyBatchWriteRequest(requestItems).map(r => doBatch(r));
            Promise.all(ps)
                .then(results => resolve(true))
                .catch(err => reject(err));
        });
    }

    batchGet(requestItems) {

        let doBatch = (requestItems, attempts) => {
            return new Promise((resolve, reject) => {
                this.client.batchGet({
                    RequestItems: requestItems
                }, (err, data) => {
                    if (err)
                        return reject(err);
                    let responses = data.Responses;
                    if (data.UnprocessedKeys) {
                        if (Object.keys(data.UnprocessedKeys).length > 0) {
                            if (!attempts || attempts < 0)
                                attempts = 0;
                            if (attempts < MAX_READ_ATTEMPTS) {
                                setTimeout(() => {
                                    doBatch(data.UnprocessedKeys, attempts + 1)
                                        .then(bgResult => {
                                            for (let tableName in bgResult) {
                                                bgResult[tableName].forEach(item => {
                                                    if (!responses[tableName])
                                                        responses[tableName] = [];
                                                    responses[tableName].push(item);
                                                });
                                            }
                                            return resolve(responses);
                                        })
                                        .catch(err => reject(err));
                                }, Math.pow(2, attempts) * 100);
                                return;
                            } else
                                console.log(`[DynamoUtils.batchGet] - alguns itens não puderam ser lidos: ${JSON.stringify(data.UnprocessedKeys)}`);
                            return reject({
                                code: 'CAPACIDADE_LEITURA_INSUFICIENTE',
                                message: 'não foi possível ler todos os dados'
                            });
                        } else
                            delete data.UnprocessedKeys;
                    }
                    return resolve(responses);
                });
            });
        }

        return new Promise((resolve, reject) => {
            let ps = this._splitifyBatchGetRequest(requestItems).map(r => doBatch(r));
            Promise.all(ps)
                .then(results => {
                    resolve(results.reduce((totalResult, result) => {
                        Object.keys(result).forEach(tableName => {
                            if (!totalResult[tableName])
                                totalResult[tableName] = [];
                            totalResult[tableName] = totalResult[tableName].concat(result[tableName]);
                        })
                        return totalResult;
                    }, {}));
                })
                .catch(err => reject(err));
        });
    }
}

module.exports = DynamoUtils;