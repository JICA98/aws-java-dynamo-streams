# DynamoStreams

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
         <version>0.0.1</version>
     </dependency>
   ````
    b. build.gradle
    
    ````groovy
   implementation group: 'io.github.jica98', name: 'aws-java-dynamo-streams', version: '0.0.1'
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
    protected DynamoStreams<Object> dynamoStreams(AmazonDynamoDBStreams dynamoDBStreams) {
        return new DynamoStreams<>(
                StreamRequest.<Object>builder()
                        .type(Object.class)
                        .dynamoDBStreams(dynamoDBStreams)
                        .streamARN(STREAM_ARN)
                .build());
    }
   ````
   
3. Now, in one of your services, subscribe to the events of your table

    ````java
    @Autowired
    private DynamoStreams<Object> dynamoStreams;
   
    @PostConstruct
    void postConstruct() {
        dynamoStreams.subscribe(event -> log.debug("{}", event));
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