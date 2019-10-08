Check input topic

    ./kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --group test1 --topic transactions 
    ./kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --group test1 --topic purchases 
    
Delete input topic

    ./kafka-topics --zookeeper localhost:2181 --delete --topic transactions

Delete all topics

    ./kafka-topics --zookeeper localhost:2181 --list | grep -v offsets | xargs -p -n 1 ./kafka-topics --zookeeper localhost:2181 --delete --topic

Reset reader consumer group

    ./kafka-consumer-groups --reset-offsets --execute --to-earliest --bootstrap-server localhost:9092 --topic transactions --group test1

Reset streams app

    ./kafka-streams-application-reset --bootstrap-servers localhost:9092 --input-topics transactions --application-id zmart-app

