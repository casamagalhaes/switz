const DEFAULT_MAX_READ_ATTEMPTS = 10;
const DEFAULT_MAX_WRITE_ATTEMPTS = 10;

class DynamoUtils {

    constructor(client, config) {
        this.client = client;
        this.config = {
            maxReadAttempts: config && config.maxReadAttempts !== undefined ? config.maxReadAttempts : DEFAULT_MAX_READ_ATTEMPTS,
            maxWriteAttempts: config && config.maxWriteAttempts !== undefined ? config.maxWriteAttempts : DEFAULT_MAX_WRITE_ATTEMPTS
        };
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

    batchWriteWithConfirmation(requestItems) {

        let doBatch = (requestItems, attempts) => {
            return new Promise((resolve, reject) => {
                this.client.batchWrite({
                    RequestItems: requestItems
                }, (err, data) => {
                    if (err) {
                        console.log(`[DynamoUtils.batchWrite] - erro ao processar itens => ${err}`);
                        return resolve({
                            success: false,
                            unprocessedItems: requestItems
                        });
                    }
                    if (data.UnprocessedItems) {
                        if (Object.keys(data.UnprocessedItems).length > 0) {
                            if (!attempts || attempts < 0)
                                attempts = 0;
                            if (attempts < this.config.maxWriteAttempts) {
                                setTimeout(() => {
                                    doBatch(data.UnprocessedItems, attempts + 1)
                                        .then(attemptResult => {
                                            if (attemptResult.success) {
                                                let attemptData = attemptResult.data;
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
                                                return resolve({
                                                    success: true,
                                                    data: data
                                                });
                                            } else {
                                                return resolve(attemptResult);
                                            }
                                        })
                                        .catch(err => {
                                            console.log(`[DynamoUtils.batchWrite] - erro ao processar itens rejeitados na tentativa anterior => ${err}`);
                                            return resolve({
                                                success: false,
                                                unprocessedItems: data.UnprocessedItems
                                            })
                                        });
                                }, Math.pow(2, attempts) * 100);
                                return;
                            } else {
                                console.log(`[DynamoUtils.batchWrite] - alguns itens não puderam ser escritos: ${JSON.stringify(data.UnprocessedItems)}`);
                                return resolve({
                                    success: false,
                                    unprocessedItems: data.UnprocessedItems
                                });
                            }
                        } else
                            delete data.UnprocessedItems;
                    }
                    if (attempts === undefined)
                        console.log('[DynamoUtils.batchWrite] - lote submetido pela tentativa 1 gravado com sucesso');
                    return resolve({
                        success: true,
                        data: data
                    });
                });
            });
        }

        let process = (requests) => {
            return new Promise((resolve, reject) => {
                if (requests.length == 0)
                    return resolve([]);
                let piece = requests.splice(0, 1000);
                let ps = piece.map(r => doBatch(r));
                Promise.all(ps)
                    .then(results => {
                        process(requests)
                            .then(results2 => resolve(results.concat(results2)))
                            .catch(err => reject(err));
                    })
                    .catch(err => reject(err));
            });
        }

        return new Promise((resolve, reject) => {
            process(this._splitifyBatchWriteRequest(requestItems))
                .then(results => resolve(results.reduce((result, r) => {
                    if (!r.success) {
                        result.success = false;
                        result.unprocessedItems = result.unprocessedItems || {};
                        Object.keys(r.unprocessedItems).forEach(tableName => {
                            if (!result.unprocessedItems[tableName])
                                result.unprocessedItems[tableName] = [];
                            result.unprocessedItems[tableName] = result.unprocessedItems[tableName].concat(r.unprocessedItems[tableName]);
                        })
                    }
                    return result;
                }, {
                    success: true
                })))
                .catch(err => reject(err));
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
                            if (attempts < this.config.maxWriteAttempts) {
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

        let process = (requests) => {
            return new Promise((resolve, reject) => {
                if (requests.length == 0)
                    return resolve([]);
                let piece = requests.splice(0, 1000);
                let ps = piece.map(r => doBatch(r));
                Promise.all(ps)
                    .then(() => process(requests))
                    .then(() => resolve(true))
                    .catch(err => reject(err));
            });
        }

        return new Promise((resolve, reject) => {
            process(this._splitifyBatchWriteRequest(requestItems))
                .then(() => resolve(true))
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
                            if (attempts < this.config.maxReadAttempts) {
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