import {
    KinesisClient,
    ListStreamsCommand,
    ListShardsCommand,
    SubscribeToShardCommand,
    RegisterStreamConsumerCommand,
    ListStreamConsumersCommand
} from '@aws-sdk/client-kinesis';
import { execSync } from 'child_process';

const kinesis = new KinesisClient({ region: 'eu-west-1' });
const STOP_PROCCESS_TIME = 5;

function stopProccess () {
    execSync(`sleep ${STOP_PROCCESS_TIME}`)
    return;
}

function listKinesisStreams (params = {}) {
    const command = new ListStreamsCommand(params);

    return kinesis.send(command);
}

function listShards (params = {}) {
    const command = new ListShardsCommand(params);

    return kinesis.send(command).then(data => data.Shards.map((sharData) => sharData.ShardId));

}

function listConsumers (params = {}) {
    const command = new ListStreamConsumersCommand(params);

    return kinesis.send(command);
}

function registerConsumer (params = {}) {
    const command = new RegisterStreamConsumerCommand(params);

    return kinesis.send(command);
}

function createSubscription (shardId, consumerARN) {
    const params = {
        ConsumerARN: consumerARN,
        ShardId: shardId,
        StartingPosition: {
            Type: 'TRIM_HORIZON'
        }
    }

    const command = new SubscribeToShardCommand(params);
    return kinesis.send(command);
}

async function readDataFromSubscription (subscription) {
    const textDecoder = new TextDecoder();
    for await (const data of subscription.EventStream) {
        for (const buffer of data.SubscribeToShardEvent.Records) {
            console.log('NNN ',  textDecoder.decode(buffer.Data));
        }
    }
}

async function main (streamName) {
    let consumerARN = '';

    const consumerName = `${streamName}-consumer`;
    const searchStreamParams = {
        StreamName: streamName,
        Limit: 10
    };

    const listConsumersParams = {
        StreamARN: '',
    }

    const registerConsumerParams = {
        StreamARN: '',
        ConsumerName: consumerName
    };

    const listShardsParams = {
        StreamARN: ''
    };

    const streamsData = await listKinesisStreams(searchStreamParams);

    if (streamsData.StreamNames.length === 0) {
        console.log('No streams found');
        return;
    }

    listConsumersParams.StreamARN = streamsData.StreamSummaries[0].StreamARN;
    const consumersData = await listConsumers(listConsumersParams);
    console.log('NNN consumersData: ', consumersData);

    if (consumersData.Consumers?.length === 0 && !consumersData.Consumers?.find((consumer) => consumer.ConsumerName === consumerName)) {
        registerConsumerParams.StreamARN = streamsData.StreamSummaries[0].StreamARN;
        const resgisterConsumerResult = await registerConsumer(registerConsumerParams);
        consumerARN = resgisterConsumerResult.Consumer.ConsumerARN;
        stopProccess();
    } else {
        consumerARN = consumersData.Consumers?.find((consumer) => consumer.ConsumerName === consumerName)?.ConsumerARN;
    }

    console.log('NNN consumerARN: ', consumerARN);

    listShardsParams.StreamARN = streamsData.StreamSummaries[0].StreamARN;
    const shardsData = await listShards(listShardsParams);
    const subscriptions = await Promise.all(shardsData.map((shardId) => createSubscription(shardId, consumerARN)));
    console.log('NNN subscriptions: ', subscriptions);
    
    await Promise.all(subscriptions.map(readDataFromSubscription))
}

main(process.argv[2]).catch(console.error);