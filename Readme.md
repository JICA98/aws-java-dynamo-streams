# DynamoStreams
[![Apache License](https://img.shields.io/github/license/JICA98/aws-java-dynamo-streams)](https://github.com/JICA98/aws-java-dynamo-streams/blob/psycho/LICENSE)

1. Provides `subscribe` method to directly listen to your dynamoDb events.
2. Listen to selected events such as `INSERT`, `REMOVE`, etc.
3. Provide your own custom `Executor` to perform stream operations in parallel.
4. Option to automatic/manual polling of DynamoDB Events.

### Steps to Set up

1. Add the dependency in your pom.xml or gradle file:

   a. pom.xml

    ````xml
    <dependency>
         <groupId>io.github.jica98</groupId>
         <artifactId>aws-java-dynamo-streams</artifactId>
         <version>0.0.4</version>
     </dependency>
   ````
    b. build.gradle
    
    ````groovy
   implementation group: 'io.github.jica98', name: 'aws-java-dynamo-streams', version: '0.0.4'
   ````
2. If you are using spring, add the following beans to your configuration class.

    ````java
   private static final String STREAM_ARN = "arn:aws:dynamodb:us-east-1:your-dynamo-db-stream";
   
    @Bean(destroyMethod = "shutdown")
    protected AmazonDynamoDBStreams streamsClient() {
        return AmazonDynamoDBStreamsClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
    }

    @Bean(destroyMethod = "shutdown")
    protected DynamoStreams<DataRoot> dynamoStreams(AmazonDynamoDBStreams dynamoDBStreams) {
        return new DynamoStreams<>(
                StreamConfig.<DataRoot>builder()
                        .clazz(DataRoot.class)
                        .dynamoDBStreams(dynamoDBStreams)
                        .streamARN(STREAM_ARN)
                .build());
    }
   ````
   
3. Now, in one of your controller/services, subscribe to the events of your table

    ````java
    @Autowired
    private DynamoStreams<DataRoot> dynamoStreams;
   
    @PostConstruct
    void postConstruct() {
        // Initialize here to start streaming events
        dynamoStreams.initialize();
    }
   
    // And return the flux in one of your endpoints
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<DataRoot>> streamData() {
        return dynamoStreams.emitter()
                .newImages()
                .map(data -> ServerSentEvent.<DataRoot>builder()
                        .data(data)
                        .id(UUID.randomUUID().toString())
                        .build());
    }
   ````
   
### Note
1. For performing the streaming, you will need the following actions defined in your policy:
   ```json
   {
      "Version": "2012-10-17",
      "Statement": [
         {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
               "dynamodb:DescribeStream",
               "dynamodb:GetShardIterator",
               "dynamodb:GetRecords"
            ],
            "Resource": "*"
         }
      ]
   }
   ```