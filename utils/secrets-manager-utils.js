const load = (opts) => {
    opts = opts || {};
    const secretId = opts.secretId !== undefined ? opts.secretId : undefined;
    const raiseException = opts.raiseException !== undefined ? opts.raiseException : true;
    const debug = opts.debug !== undefined ? opts.debug : false;
    const parseStringSecretToJSON = opts.parseStringSecretToJSON !== undefined ? opts.parseStringSecretToJSON : true;
    const region = opts.region !== undefined ? opts.region : undefined;
    const binarySecretDefaultKey = opts.binarySecretDefaultKey !== undefined ? opts.binarySecretDefaultKey : 'SMLOADER_BINARY_SECRET';
    const stringSecretDefaultKey = opts.stringSecretDefaultKey !== undefined ? opts.stringSecretDefaultKey : 'SMLOADER_STRING_SECRET';

    const getEnvVar = (name, settings) => {
        settings = settings || {};
        const boolType = settings.boolType !== undefined ? settings.boolType : false;
        const defaultValue = settings.defaultValue !== undefined ? settings.defaultValue : false;
        if (name in process.env) {
            if (boolType) return process.env[name].toLowerCase() === 'true';
            else return process.env[name];
        }
        return defaultValue;
    }

    const SMLOADER_DEBUG = getEnvVar('SMLOADER_DEBUG', { boolType: true, defaultValue: debug });
    const SMLOADER_SECRET_ID = getEnvVar('SMLOADER_SECRET_ID', { defaultValue: secretId });
    const SMLOADER_RAISE_EXCEPTION = getEnvVar('SMLOADER_RAISE_EXCEPTION', { boolType: true, defaultValue: raiseException });
    const SMLOADER_PARSE_STRING_SECRET_TO_JSON = getEnvVar('SMLOADER_PARSE_STRING_SECRET_TO_JSON', { boolType: true, defaultValue: parseStringSecretToJSON });
    const SMLOADER_BINARY_SECRET_DEFAULT_KEY = getEnvVar('SMLOADER_BINARY_SECRET_DEFAULT_KEY', { defaultValue: binarySecretDefaultKey });
    const SMLOADER_STRING_SECRET_DEFAULT_KEY = getEnvVar('SMLOADER_STRING_SECRET_DEFAULT_KEY', { defaultValue: stringSecretDefaultKey });
    const SMLOADER_AWS_DEFAULT_REGION = getEnvVar('SMLOADER_AWS_DEFAULT_REGION', { defaultValue: region || getEnvVar('AWS_DEFAULT_REGION') });

    const finish = err => {
        if (SMLOADER_RAISE_EXCEPTION) {
            if (err instanceof String) throw new Error(err);
            else throw err;
        }
        if (SMLOADER_DEBUG) {
            console.log('Done');
        }
    }

    if (SMLOADER_DEBUG) {
        console.log('Initializing SM loader');
        console.log(`SMLOADER_DEBUG=${SMLOADER_DEBUG}`);
        console.log(`SMLOADER_SECRET_ID=${SMLOADER_SECRET_ID}`);
        console.log(`SMLOADER_RAISE_EXCEPTION=${SMLOADER_RAISE_EXCEPTION}`);
        console.log(`SMLOADER_PARSE_STRING_SECRET_TO_JSON=${SMLOADER_PARSE_STRING_SECRET_TO_JSON}`);
        console.log(`SMLOADER_BINARY_SECRET_DEFAULT_KEY=${SMLOADER_BINARY_SECRET_DEFAULT_KEY}`);
        console.log(`SMLOADER_STRING_SECRET_DEFAULT_KEY=${SMLOADER_STRING_SECRET_DEFAULT_KEY}`);
        console.log(`SMLOADER_AWS_DEFAULT_REGION=${SMLOADER_AWS_DEFAULT_REGION}`);
    }

    if (!SMLOADER_SECRET_ID) return finish('No secretId passed to secret manager loader');

    const parseStringSecretToEnv = data => {
        const json = JSON.parse(data);
        const keys = Object.keys(json);
        for (key of keys) {
            process.env[key] = json[key];
        }
    }

    const AWS = require('aws-sdk');
    const client = new AWS.SecretsManager({
        region: SMLOADER_AWS_DEFAULT_REGION
    });
    return new Promise((resolve, reject) => {
        client.getSecretValue({
            SecretId: SMLOADER_SECRET_ID
        }, (err, data) => {
            if (err) return reject(err);
            if ('SecretString' in data) {
                const secret = data.SecretString;
                process.env[SMLOADER_STRING_SECRET_DEFAULT_KEY] = secret;
                if (SMLOADER_PARSE_STRING_SECRET_TO_JSON) {
                    try {
                        parseStringSecretToEnv(secret);
                    } catch (error) {
                        return reject(error);
                    }
                }
            } else {
                const buff = Buffer.from(data.SecretBinary, 'base64');
                const decodedBinarySecret = buff.toString('ascii');
                process.env[SMLOADER_BINARY_SECRET_DEFAULT_KEY] = decodedBinarySecret;
            }
            return resolve();
        });
    });
};

module.exports = {
    load
}